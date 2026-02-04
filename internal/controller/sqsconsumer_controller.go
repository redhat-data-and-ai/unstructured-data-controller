/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

const (
	SQSConsumerControllerName         = "SQSConsumer"
	intentionalReconcileAgainDuration = 15 * time.Second
)

// Supported formats for docling document processing
var doclingSupportedFormats = []string{"pdf", "md", "docx", "pptx"}

// SQSConsumerReconciler reconciles a SQSConsumer object
type SQSConsumerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// SQSMessage represents an S3 event notification received via SQS
type SQSMessage struct {
	Records []S3EventRecord `json:"Records"`
}

// S3EventRecord represents a single S3 event record
type S3EventRecord struct {
	AWSRegion    string              `json:"awsRegion"`
	EventTime    string              `json:"eventTime"`
	UserIdentity S3EventUserIdentity `json:"userIdentity"`
	S3           S3EventData         `json:"s3"`
}

func (r *S3EventRecord) validate(ctx context.Context) bool {
	logger := log.FromContext(ctx)

	// check if the bucket is the ingestion bucket
	if r.S3.Bucket.Name != ingestionBucket {
		logger.Info("skipping record", "key", r.S3.Object.Key, "got bucket name", r.S3.Bucket.Name, "expected bucket name", ingestionBucket)
		return false
	}

	// verify the file actually exists
	// it's possible that the file is deleted after the message is received but before the message is processed
	exists, err := awsclienthandler.ObjectExists(ctx, r.S3.Bucket.Name, r.S3.Object.Key)
	if err != nil {
		logger.Error(err, "failed to check if object exists", "key", r.S3.Object.Key)
		return false
	}
	if !exists {
		logger.Info("skipping record", "key", r.S3.Object.Key, "object does not exist")
		return false
	}

	// example record.s3.object.key: bookingsmaster/pexels.pdf
	recordExt := strings.ReplaceAll(path.Ext(r.S3.Object.Key), ".", "")
	if !slices.Contains(doclingSupportedFormats, recordExt) {
		logger.Info("skipping record", "key", r.S3.Object.Key, "file not in supported formats", recordExt, "supported formats", doclingSupportedFormats)
		return false
	}

	filePathSplit := strings.Split(r.S3.Object.Key, "/")

	// length of filePathSplit should be at least 3, ["dataproduct","rest/of/the/path"]
	if len(filePathSplit) < 2 {
		logger.Info("skipping record", "key", r.S3.Object.Key, "message", "file path should be at least 2 levels deep like 'bucket/prefix/filename'")
		return false
	}

	return true
}

type S3EventUserIdentity struct {
	PrincipalID string `json:"principalId"`
}

// S3EventData contains the S3-specific event data
type S3EventData struct {
	Bucket S3EventBucket `json:"bucket"`
	Object S3EventObject `json:"object"`
}

type S3EventBucket struct {
	Name string `json:"name"`
	ARN  string `json:"arn"`
}

type S3EventObject struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sqsconsumers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sqsconsumers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sqsconsumers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",namespace=unstructured-controller-namespace,resources=secrets,verbs=get;list;watch

// Reconcile reconciles the SQSConsumer CR. It follows these steps:
// - receive messages from the SQS queue
// - process the messages
// - add a force reconcile label to the unstructured data product
// - delete the message from the SQS queue

func (r *SQSConsumerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// if isControllerDisabled(SQSConsumerControllerName) {
	// 	logger.Info("SQSConsumer controller is disabled, skipping reconciliation")
	// 	return ctrl.Result{}, nil // no error, just skip the reconciliation
	// }

	// check if config CR is healthy
	// TODO: Uncomment when Config CR is available
	// isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	// if err != nil {
	// 	logger.Error(err, "failed to check if Config CR is healthy")
	// 	return ctrl.Result{}, err
	// }
	//
	// if !isHealthy {
	// 	logger.Info("Config CR is not ready yet, will try again in a bit ...")
	// 	return ctrl.Result{
	// 		RequeueAfter: 10 * time.Second,
	// 	}, nil
	// }

	sqsConsumerCR := &operatorv1alpha1.SQSConsumer{}
	if err := r.Get(ctx, req.NamespacedName, sqsConsumerCR); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	key := client.ObjectKeyFromObject(sqsConsumerCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.SQSConsumer{}
		if err := r.Get(ctx, key, res); err != nil {
			return err
		}
		res.SetWaiting()
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update SQSConsumer status")
		return ctrl.Result{}, err
	}

	sqsClient, err := awsclienthandler.GetSQSClient()
	if err != nil {
		return r.handleError(ctx, sqsConsumerCR, err)
	}

	output, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &sqsConsumerCR.Spec.QueueURL,
		MaxNumberOfMessages: 10,
		// wait for 15 seconds to get the messages, should be decent enough
		WaitTimeSeconds:   15,
		VisibilityTimeout: 300,
	})
	if err != nil {
		return r.handleError(ctx, sqsConsumerCR, err)
	}

	for _, message := range output.Messages {
		if messageError := r.processMessage(ctx, message, req.Namespace); len(messageError) > 0 {
			for _, err := range messageError {
				logger.Error(err, "failed to process message", "MessageId", *message.MessageId)
				// we will not `continue` here, we will let this continue and the message will be deleted from the queue
			}
		}

		// delete the message from the queue
		if _, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      &sqsConsumerCR.Spec.QueueURL,
			ReceiptHandle: message.ReceiptHandle,
		}); err != nil {
			logger.Error(err, "failed to delete message from queue")
		}
		logger.Info("deleted message from queue", "MessageId", *message.MessageId)
	}

	if err := controllerutils.RemoveForceReconcileLabel(ctx, r.Client, sqsConsumerCR); err != nil {
		logger.Error(err, "error removing the force-reconcile label from the SQSConsumer CR")
		return ctrl.Result{}, err
	}

	// reconcile again after some time to check if there are any more messages to read
	logger.Info("successfully processed all messages, will check again ...")
	successMessage := "successfully processed all messages, will check again ..."
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.SQSConsumer{}
		if err := r.Get(ctx, key, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update SQSConsumer status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{
		Requeue:      true,
		RequeueAfter: intentionalReconcileAgainDuration,
	}, nil
}

func (r *SQSConsumerReconciler) processMessage(ctx context.Context, message sqstypes.Message, namespace string) []error {
	logger := log.FromContext(ctx)
	logger.Info("received message", "MessageId", *message.MessageId)

	errorList := []error{}

	// parse the message
	var sqsMessage SQSMessage
	if err := json.Unmarshal([]byte(*message.Body), &sqsMessage); err != nil {
		logger.Error(err, "failed to unmarshal message")
		errorList = append(errorList, err)
		return errorList
	}

	// every message may contain multiple records and each record points to a file
	// we don't want to block the reconciliation if one of the records cannot be processed
	// so we will continue to process the other records
	for _, record := range sqsMessage.Records {
		// the file name is URL encoded
		objectKey, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			logger.Error(err, "failed to unescape key", "key", record.S3.Object.Key)
			errorList = append(errorList, err)
			continue
		}

		// validate the record first
		if !record.validate(ctx) {
			logger.Info("unable to validate record, skipping ...", "key", objectKey)
			errorList = append(errorList, errors.New("unable to validate record, key: "+objectKey))
			continue
		}

		sourceFilePathSplit := strings.Split(objectKey, "/")
		dataProductName := sourceFilePathSplit[0]

		// now apply a force reconcile label to the unstructured data product
		unstructuredDataProduct := &operatorv1alpha1.UnstructuredDataProduct{}
		if err := r.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      dataProductName,
		}, unstructuredDataProduct); err != nil {
			logger.Error(err, "failed to get unstructured data product", "key", objectKey)
			errorList = append(errorList, err)
			continue
		}
		if err := controllerutils.AddForceReconcileLabel(ctx, r.Client, unstructuredDataProduct); err != nil {
			logger.Error(err, "failed to add force reconcile label", "key", objectKey)
			errorList = append(errorList, err)
			continue
		}

		logger.Info("successfully processed record", "key", objectKey)
	}
	return errorList
}

func (r *SQSConsumerReconciler) handleError(ctx context.Context, sqsConsumerCR *operatorv1alpha1.SQSConsumer, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	key := client.ObjectKeyFromObject(sqsConsumerCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.SQSConsumer{}
		if getErr := r.Get(ctx, key, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update SQSConsumer status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

// SetupWithManager sets up the controller with the Manager.
func (r *SQSConsumerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.SQSConsumer{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Named("sqsconsumer").
		Complete(r)
}
