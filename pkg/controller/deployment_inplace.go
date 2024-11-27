// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sort"

	"github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/controller/autoscaler"
	labelsutil "github.com/gardener/machine-controller-manager/pkg/util/labels"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/integer"
)

// rolloutInPlace implements the logic for rolling  a machine set without replacing it.
func (dc *controller) rolloutInPlace(ctx context.Context, d *v1alpha1.MachineDeployment, isList []*v1alpha1.MachineSet, machineMap map[types.UID]*v1alpha1.MachineList) error {
	clusterAutoscalerScaleDownAnnotations := make(map[string]string)
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationValue

	// We do this to avoid accidentally deleting the user provided annotations.
	clusterAutoscalerScaleDownAnnotations[autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMKey] = autoscaler.ClusterAutoscalerScaleDownDisabledAnnotationByMCMValue

	newIS, oldISs, err := dc.getAllMachineSetsAndSyncRevision(ctx, d, isList, machineMap, true)
	if err != nil {
		return err
	}
	allISs := append(oldISs, newIS)

	err = dc.taintNodesBackingMachineSets(
		ctx,
		oldISs, &v1.Taint{
			Key:    PreferNoScheduleKey,
			Value:  "True",
			Effect: "PreferNoSchedule",
		},
	)

	if dc.autoscalerScaleDownAnnotationDuringRollout {
		// Add the annotation on the all machinesets if there are any old-machinesets and not scaled-to-zero.
		// This also helps in annotating the node under new-machineset, incase the reconciliation is failing in next
		// status-rollout steps.
		if len(oldISs) > 0 && !dc.machineSetsScaledToZero(oldISs) {
			// Annotate all the nodes under this machine-deployment, as roll-out is on-going.
			err := dc.annotateNodesBackingMachineSets(ctx, allISs, clusterAutoscalerScaleDownAnnotations)
			if err != nil {
				klog.Errorf("Failed to add %s on all nodes. Error: %s", clusterAutoscalerScaleDownAnnotations, err)
				return err
			}
		}
	}

	if err != nil {
		klog.Warningf("Failed to add %s on all nodes. Error: %s", PreferNoScheduleKey, err)
	}

	// In this section, we will attempt to scale up the new machine set. Machines with the `machine.sapcloud.io/update-successful` label
	// can transfer their ownership to the new machine set.
	// It is crucial to ensure that during the ownership transfer, the machine is not deleted,
	// and the old machine set is not scaled up to recreate the machine.
	scaledUp, err := dc.reconcileNewMachineSetInPlace(ctx, oldISs, newIS, d)
	if err != nil {
		klog.V(3).Infof("this was unexpected error")
		return err
	}
	if scaledUp {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allISs, newIS, d)
	}

	// prepare old ISs for update
	machinesSelectedForUpdate, err := dc.reconcileOldMachineSetsInPlace(ctx, allISs, FilterActiveMachineSets(oldISs), newIS, d)
	if err != nil {
		return err
	}
	if machinesSelectedForUpdate {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allISs, newIS, d)
	}

	if MachineDeploymentComplete(d, &d.Status) {
		if dc.autoscalerScaleDownAnnotationDuringRollout {
			// Check if any of the machine under this MachineDeployment contains the by-mcm annotation, and
			// remove the original autoscaler annotation only after.
			err := dc.removeAutoscalerAnnotationsIfRequired(ctx, allISs, clusterAutoscalerScaleDownAnnotations)
			if err != nil {
				return err
			}
		}
		if err := dc.cleanupMachineDeployment(ctx, oldISs, d); err != nil {
			return err
		}
	}

	// Sync deployment status
	return dc.syncRolloutStatus(ctx, allISs, newIS, d)
}

func (dc *controller) reconcileNewMachineSetInPlace(ctx context.Context, oldISs []*v1alpha1.MachineSet, newIS *v1alpha1.MachineSet, deployment *v1alpha1.MachineDeployment) (bool, error) {
	machines, err := dc.machineLister.List(labels.SelectorFromSet(newIS.Spec.Selector.MatchLabels))
	if err != nil {
		return false, err
	}

	machinesWithUpdateSuccessfulLabel := filterMachinesWithUpdateSuccessfulLabel(machines)

	if len(machines) > int(newIS.Spec.Replicas) && len(machinesWithUpdateSuccessfulLabel) > 0 {
		// scale up the new machine set to the number of machines with the update successful label.
		scaled, _, err := dc.scaleMachineSetAndRecordEvent(ctx, newIS, newIS.Spec.Replicas+int32(len(machinesWithUpdateSuccessfulLabel)), deployment)
		return scaled, err
	}

	// remove all the label from the machines related to the inplace update.
	for _, machine := range machinesWithUpdateSuccessfulLabel {
		labels := machine.Labels
		delete(labels, v1alpha1.LabelKeyMachineSelectedForUpdate)
		delete(labels, v1alpha1.LabelKeyMachineIsReadyForUpdate)
		delete(labels, v1alpha1.LabelKeyMachineUpdateSuccessful)
		addLabelPatch := fmt.Sprintf(`{"metadata":{"labels":{%s}}}`, labelsutil.GetFormatedLabels(labels))

		klog.V(3).Infof("removing label from machine %s update-successful %s", machine.Name, labels)
		if err := dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addLabelPatch)); err != nil {
			klog.V(3).Infof("error while removing label update-successful %s", err)
			return false, err
		}
	}

	// uncordon the node if the for the machine with the update successful label.
	for _, machine := range machinesWithUpdateSuccessfulLabel {
		nodeName, ok := machine.Labels[v1alpha1.NodeLabelKey]
		if !ok {
			return false, fmt.Errorf("node label not found for machine %s", machine.Name)
		}

		node, err := dc.nodeLister.Get(nodeName)
		if err != nil {
			return false, fmt.Errorf("failed to get node %s", nodeName)
		}

		node.Spec.Unschedulable = false
		_, err = dc.controlCoreClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to uncordon the node %s", node.Name)
		}
	}

	if (newIS.Spec.Replicas) == (deployment.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if (newIS.Spec.Replicas) > (deployment.Spec.Replicas) {
		// Scale down.
		scaled, _, err := dc.scaleMachineSetAndRecordEvent(ctx, newIS, (deployment.Spec.Replicas), deployment)
		return scaled, err
	}

	klog.V(3).Infof("reconcile new machine set %s", newIS.Name)

	addedNewReplicasCount := int32(0)

	for _, is := range oldISs {
		transfferedMachineCount := int32(0)
		// get the machines for the machine set
		machines, err := dc.machineLister.List(labels.SelectorFromSet(is.Spec.Selector.MatchLabels))
		if err != nil {
			return false, nil
		}

		klog.V(3).Infof("machine in old machine set %s: %d", is.Name, len(machines))

		// select the nodes which has been updated successfully
		// TODO: use the metadate only lister for the nodes. (if possible)
		nodes, err := dc.nodeLister.List(labels.SelectorFromSet(map[string]string{v1alpha1.LabelKeyMachineUpdateSuccessful: "true"}))
		if err != nil {
			return false, nil
		}

		klog.V(3).Infof("nodes with update successful %v: %d", nodes, len(nodes))

		for _, node := range nodes {
			machine, err := getMachineFromNode(machines, node)
			// ignore error, if machine not found for the node.
			if err != nil {
				continue
			}

			klog.V(3).Infof("found machine for updated node, machine: %s, node: %s", machine.Name, node.Name)

			// removes labels not present in newIS so that the machine is not selected by the old machine set
			machineNewLabels := MergeStringMaps(MergeWithOverwriteAndFilter(machine.Labels, is.Spec.Selector.MatchLabels, newIS.Spec.Selector.MatchLabels), map[string]string{v1alpha1.LabelKeyMachineUpdateSuccessful: "true"})

			// update the owner reference of the machine to the new machine set and update the labels
			addControllerPatch := fmt.Sprintf(
				`{"metadata":{"ownerReferences":[{"apiVersion":"machine.sapcloud.io/v1alpha1","kind":"%s","name":"%s","uid":"%s","controller":true,"blockOwnerDeletion":true}],"labels":{%s},"uid":"%s"}}`,
				v1alpha1.SchemeGroupVersion.WithKind("MachineSet"),
				newIS.GetName(), newIS.GetUID(), labelsutil.GetFormatedLabels(machineNewLabels), machine.UID)
			err = dc.machineControl.PatchMachine(ctx, machine.Namespace, machine.Name, []byte(addControllerPatch))

			// TODO: add condition in the machine set controller that don't down scale the machine set if the machine is updated successfully.
			// what if there is no error, can it lead to machine deletion in the new machine set since the set will have more replicas.
			// the set in the replica field in the machine set.
			// mostly will have to adapt the logic that during the inplace take care of these stuff.
			// which can lead to scary situations.
			if err != nil {
				klog.V(3).Infof("failed to transfer the ownership of machine to new machine set %s", err)
				// scale up the new machine set to the already added replicas.
				scaled, _, err2 := dc.scaleMachineSetAndRecordEvent(ctx, newIS, newIS.Spec.Replicas+addedNewReplicasCount, deployment)
				// if things get errored here can it lead to machine deletion
				// not sure mostly not since the owner reference is not updated.
				// and nor it has been scaled up or down.
				klog.V(3).Infof("scale up after failure failed %s", err2)
				klog.V(3).Infof("scale up replica count after failure %d", addedNewReplicasCount)
				if err2 != nil {
					return addedNewReplicasCount > 0, err2
				}

				klog.V(3).Infof("scale down machine set %s, transffered replicas to new machine set %d", is.Name, transfferedMachineCount)
				_, _, err2 = dc.scaleMachineSetAndRecordEvent(ctx, is, is.Spec.Replicas-transfferedMachineCount, deployment)
				klog.V(3).Infof("scale down after failure failed %s", err2)
				if err2 != nil {
					klog.V(3).Infof("scale down failed %s", err)
					return addedNewReplicasCount > 0, err
				}

				return scaled, err
			}

			// uncordon the node since the ownership of the machine has been transferred to the new machien set.
			node.Spec.Unschedulable = false
			_, err = dc.controlCoreClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to uncordon the node %s", node.Name)
			}

			transfferedMachineCount++ // scale down the old machine set.
			addedNewReplicasCount++   // scale up the new machine set.
		}

		// TODO: add the logic to down scale the old machiene set.
		// can we safely do it, can it lead to machine deletion.
		// but since(maybe) the machine has been shifted to the new machine set, it should be safe to do it.
		// TODO: add label on machine set before transfering the ownership of the machine so mcs controller know that it should not upscale the old machine set.
		klog.V(3).Infof("scale down machine set %s, transffered replicas to new machine set %d", is.Name, transfferedMachineCount)
		_, _, err = dc.scaleMachineSetAndRecordEvent(ctx, is, is.Spec.Replicas-transfferedMachineCount, deployment)
		if err != nil {
			klog.V(3).Infof("scale down failed %s", err)
			return addedNewReplicasCount > 0, err
		}
	}

	klog.V(3).Infof("scale up the new machine set %s by %d", newIS.Name, addedNewReplicasCount)
	scaled, _, err := dc.scaleMachineSetAndRecordEvent(ctx, newIS, newIS.Spec.Replicas+addedNewReplicasCount, deployment)
	return scaled, err
}

