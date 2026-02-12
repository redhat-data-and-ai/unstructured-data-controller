package controller

import (
	"context"
	"strings"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AWS client "not initialized" error messages (from pkg/awsclienthandler).
const (
	errMsgS3NotInitialized      = "S3 client not initialized yet"
	errMsgSQSNotInitialized     = "SQS client not initialized yet"
	errMsgPresignNotInitialized = "presign client not initialized yet"
)

// IsAWSClientNotInitializedError returns true if err indicates an AWS client (S3, SQS, or presign)
// is not initialized yet. Used to requeue without error when ControllerConfig has not run yet at startup.
func IsAWSClientNotInitializedError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, errMsgS3NotInitialized) ||
		strings.Contains(msg, errMsgSQSNotInitialized) ||
		strings.Contains(msg, errMsgPresignNotInitialized)
}

// IsControllerConfigGlobalsSet returns true if the controller package globals (cacheDirectory,
// dataStorageBucket) have been set by ControllerConfig Reconcile. When false, filestore.New
// would be called with empty paths and fail before reaching GetS3Client(), so we requeue early.
func IsControllerConfigGlobalsSet(cacheDir, dataBucket string) bool {
	return cacheDir != "" && dataBucket != ""
}

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
