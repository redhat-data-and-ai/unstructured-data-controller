package controller

import (
	"context"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func IsConfigCRHealthy(ctx context.Context, kubeClient client.Client, namespace string) (bool, error) {
	controllerConfigCR := &operatorv1alpha1.ControllerConfig{}
	if err := kubeClient.Get(ctx, types.NamespacedName{
		Name:      "controllerconfig",
		Namespace: namespace,
	}, controllerConfigCR); err != nil {
		return false, err
	}

	if !controllerConfigCR.IsHealthy() {
		return false, nil
	}

	return true, nil
}
