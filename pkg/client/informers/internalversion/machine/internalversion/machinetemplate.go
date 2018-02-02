// This file was automatically generated by informer-gen

package internalversion

import (
	time "time"

	machine "github.com/gardener/node-controller-manager/pkg/apis/machine"
	clientset_internalversion "github.com/gardener/node-controller-manager/pkg/client/clientset/internalversion"
	internalinterfaces "github.com/gardener/node-controller-manager/pkg/client/informers/internalversion/internalinterfaces"
	internalversion "github.com/gardener/node-controller-manager/pkg/client/listers/machine/internalversion"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// MachineTemplateInformer provides access to a shared informer and lister for
// MachineTemplates.
type MachineTemplateInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() internalversion.MachineTemplateLister
}

type machineTemplateInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewMachineTemplateInformer constructs a new informer for MachineTemplate type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewMachineTemplateInformer(client clientset_internalversion.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredMachineTemplateInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredMachineTemplateInformer constructs a new informer for MachineTemplate type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredMachineTemplateInformer(client clientset_internalversion.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.Machine().MachineTemplates(namespace).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.Machine().MachineTemplates(namespace).Watch(options)
			},
		},
		&machine.MachineTemplate{},
		resyncPeriod,
		indexers,
	)
}

func (f *machineTemplateInformer) defaultInformer(client clientset_internalversion.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredMachineTemplateInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *machineTemplateInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&machine.MachineTemplate{}, f.defaultInformer)
}

func (f *machineTemplateInformer) Lister() internalversion.MachineTemplateLister {
	return internalversion.NewMachineTemplateLister(f.Informer().GetIndexer())
}