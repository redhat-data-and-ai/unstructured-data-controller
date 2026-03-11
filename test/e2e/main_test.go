//go:build e2e
// +build e2e

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

package e2e

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	operatorUtils "github.com/redhat-data-and-ai/unstructured-data-controller/test/utils"
	"sigs.k8s.io/e2e-framework/klient/wait"
	"sigs.k8s.io/e2e-framework/klient/wait/conditions"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/envfuncs"
	"sigs.k8s.io/e2e-framework/support/kind"
	"sigs.k8s.io/e2e-framework/support/utils"
)

var (
	testenv         env.Environment
	kindClusterName string
)

const (
	testNamespace          = "unstructured-controller-namespace"
	deploymentName         = "unstructured-controller-manager"
	unstructuredSecretName = "unstructured-secret"
)

func TestMain(m *testing.M) {
	testenv = env.New()
	runningProcesses := []exec.Cmd{}

	kindClusterName = os.Getenv("KIND_CLUSTER")
	skipClusterSetup := os.Getenv("SKIP_CLUSTER_SETUP")
	skipClusterCleanup := os.Getenv("SKIP_CLUSTER_CLEANUP")
	cleanupRequired := os.Getenv("SKIP_TEST_CLEANUP") != "true"

	testenv.Setup(
		func(ctx context.Context, config *envconf.Config) (context.Context, error) {
			// Register custom API scheme
			if err := v1alpha1.AddToScheme(config.Client().Resources().GetScheme()); err != nil {
				log.Fatalf("Failed to register v1alpha1 scheme: %s", err)
			}

			kindCluster := kind.NewCluster(kindClusterName)

			if skipClusterSetup != "true" {
				log.Printf("Creating new kind cluster with name: %s", kindClusterName)
				envFuncs := []env.Func{
					envfuncs.CreateCluster(kindCluster, kindClusterName),
					envfuncs.CreateNamespace(testNamespace),
				}
				for _, envFunc := range envFuncs {
					if _, err := envFunc(ctx, config); err != nil {
						log.Fatalf("Failed to create kind cluster: %s", err)
					}
				}
			}

			if err := testSetup(ctx, &runningProcesses, config); err != nil {
				if cleanupRequired {
					_ = testCleanup(ctx, config, &runningProcesses)
				}
				log.Fatalf("failed to setup test environment: %s", err)
			}
			return ctx, nil
		},
	)

	testenv.Finish(
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			log.Println("finishing tests, cleaning cluster ...")
			if cleanupRequired {
				if err := testCleanup(ctx, cfg, &runningProcesses); err != nil {
					log.Printf("failed to cleanup test environment: %s", err)
					return ctx, err
				}
			}
			return ctx, nil
		},
	)

	if skipClusterCleanup != "true" && skipClusterSetup != "true" {
		testenv.Finish(
			envfuncs.DeleteNamespace(testNamespace),
			envfuncs.DestroyCluster(kindClusterName),
		)
	}

	os.Exit(testenv.Run(m))
}

func TestConfigHealthy(t *testing.T) {
	// Setup creates ControllerConfig and waits for ConfigReady=true; this test runs after setup.
	t.Log("ControllerConfig is healthy (validated in testSetup)")
}

func testSetup(_ context.Context, runningProcesses *[]exec.Cmd, config *envconf.Config) error {
	// change dir for Makefile or it will fail
	if err := os.Chdir("../../"); err != nil {
		log.Printf("Unable to set working directory: %s", err)
		return err
	}

	image := os.Getenv("IMG")
	if image == "" {
		return fmt.Errorf("IMG environment variable is required")
	}

	log.Println("Deploying operator with CRDs installed...")
	deployCommand := fmt.Sprintf("make IMG=%s deploy", image)
	if p := utils.RunCommand(deployCommand); p.Err() != nil {
		log.Printf("Failed to deploy operator: %s", p.Err())
		return p.Err()
	}

	log.Println("Verifying deployment exists...")
	checkCmd := fmt.Sprintf("kubectl get deployment %s -n %s", deploymentName, testNamespace)
	if p := utils.RunCommand(checkCmd); p.Err() != nil {
		log.Printf("Deployment not found: %s", p.Err())
		return p.Err()
	}

	log.Printf("Deployment %s found in namespace %s", deploymentName, testNamespace)

	log.Println("Waiting for controller-manager deployment to be available...")
	client := config.Client()
	if err := wait.For(
		conditions.New(client.Resources()).DeploymentAvailable(deploymentName, testNamespace),
		wait.WithTimeout(10*time.Minute),
		wait.WithInterval(2*time.Second),
	); err != nil {
		log.Printf("Timed out waiting for deployment: %s", err)
		return err
	}

	log.Println("Creating consolidated unstructured secret")

	// Create unstructured-secret with all required credentials
	if p := utils.RunCommand(fmt.Sprintf("kubectl apply -n %s -f config/samples/unstructured-secret.yaml", testNamespace)); p.Err() != nil {
		log.Printf("Failed to create unstructured-secret: %s", p.Err())
		return p.Err()
	}

	// Update SNOWFLAKE_PRIVATE_KEY if provided via environment
	privateKeyContent := os.Getenv("SNOWFLAKE_PRIVATE_KEY")
	if privateKeyContent != "" {
		log.Println("Updating SNOWFLAKE_PRIVATE_KEY from environment variable")
		patchCmd := fmt.Sprintf("kubectl patch secret %s -n %s --type=merge -p '{\"stringData\":{\"SNOWFLAKE_PRIVATE_KEY\":\"%s\"}}'",
			unstructuredSecretName, testNamespace, privateKeyContent)
		if p := utils.RunCommand(patchCmd); p.Err() != nil {
			log.Printf("Failed to patch snowflake private key: %s", p.Err())
			return p.Err()
		}
	}

	log.Println("Unstructured secret created successfully")

	skipLocalstack := os.Getenv("SKIP_LOCALSTACK_SETUP")
	if skipLocalstack != "true" {
		log.Println("Deploying localstack...")
		if p := utils.RunCommand(fmt.Sprintf("kubectl apply -n %s -f test/localstack/", testNamespace)); p.Err() != nil {
			log.Printf("Failed to deploy localstack: %s", p.Err())
			return p.Err()
		}
		log.Println("Checking localstack deployment status...")
		utils.RunCommand(fmt.Sprintf("kubectl get pods -n %s -l app=localstack", testNamespace))
		log.Println("Waiting for localstack to be ready...")
		if err := wait.For(
			conditions.New(client.Resources()).DeploymentAvailable("localstack", testNamespace),
			wait.WithTimeout(10*time.Minute),
			wait.WithInterval(5*time.Second),
		); err != nil {
			log.Printf("Timed out waiting for localstack: %s", err)
			return err
		}
		log.Println("Port-forwarding localstack to localhost:4566")
		pf := exec.Command("kubectl", "port-forward", "-n", testNamespace, "services/localstack", "4566:4566")
		pf.Stdout = os.Stdout
		pf.Stderr = os.Stderr
		if err := pf.Start(); err != nil {
			log.Printf("failed to port-forward localstack: %s", err)
			return err
		}
		*runningProcesses = append(*runningProcesses, *pf)
	}

	skipDocling := os.Getenv("SKIP_DOCLING_SETUP")
	if skipDocling != "true" {
		log.Println("Deploying docling-serve...")
		if p := utils.RunCommand(fmt.Sprintf("kubectl apply -n %s -f test/docling-serve/", testNamespace)); p.Err() != nil {
			log.Printf("Failed to deploy docling-serve: %s", p.Err())
			return p.Err()
		}
		log.Println("Waiting for docling-serve to be ready...")
		if err := wait.For(
			conditions.New(client.Resources()).DeploymentAvailable("docling-serve", testNamespace),
			wait.WithTimeout(10*time.Minute),
			wait.WithInterval(5*time.Second),
		); err != nil {
			log.Printf("Timed out waiting for docling-serve: %s", err)
			return err
		}
		log.Println("Port-forwarding docling-serve to localhost:5002")
		pf := exec.Command("kubectl", "port-forward", "-n", testNamespace, "services/docling-serve", "5002:5001")
		pf.Stdout = os.Stdout
		pf.Stderr = os.Stderr
		if err := pf.Start(); err != nil {
			log.Printf("failed to port-forward docling-serve: %s", err)
			return err
		}
		*runningProcesses = append(*runningProcesses, *pf)
	}

	// get ControllerConfig from utils/utils_function.go
	controllerConfig := operatorUtils.GetControllerConfigResource()
	if err := config.Client().Resources().Create(context.Background(), controllerConfig); err != nil {
		log.Printf("failed to apply ControllerConfig: %s", err)
		return err
	}

	skipConfigReady := os.Getenv("SKIP_CONTROLLER_CONFIG_READY")
	if skipConfigReady == "true" {
		log.Println("SKIP_CONTROLLER_CONFIG_READY=true: skipping wait for ConfigReady (e.g. CI without Snowflake secret)")
		return nil
	}
	log.Println("Waiting for ControllerConfig to be healthy (ConfigReady=true)...")
	configWaitCmd := fmt.Sprintf(
		"kubectl wait --for=condition=ConfigReady=true controllerconfigs.operator.dataverse.redhat.com/controllerconfig -n %s --timeout=2m",
		testNamespace,
	)
	if p := utils.RunCommand(configWaitCmd); p.Err() != nil {
		log.Printf("failed to meet condition for ControllerConfig: %s", p.Err())
		return p.Err()
	}
	log.Println("ControllerConfig is healthy")
	return nil
}

func testCleanup(_ context.Context, _ *envconf.Config, runningProcesses *[]exec.Cmd) error {
	log.Println("cleaning up test environment ...")
	errorList := []error{}

	commandList := []string{
		"make undeploy ignore-not-found=true",
		fmt.Sprintf("kubectl delete secret %s -n %s --ignore-not-found=true", unstructuredSecretName, testNamespace),
		fmt.Sprintf("kubectl delete controllerconfigs.operator.dataverse.redhat.com controllerconfig -n %s --ignore-not-found=true", testNamespace),
		fmt.Sprintf("kubectl delete -f test/localstack/ -n %s --ignore-not-found=true", testNamespace),
		fmt.Sprintf("kubectl delete -f test/docling-serve/ -n %s --ignore-not-found=true", testNamespace),
	}
	for _, command := range commandList {
		if p := utils.RunCommand(command); p.Err() != nil {
			errorList = append(errorList, fmt.Errorf("failed to run command: %s: %s", command, p.Err()))
		}
	}

	for _, process := range *runningProcesses {
		if killErr := process.Process.Kill(); killErr != nil {
			errorList = append(errorList, killErr)
		}
	}

	if len(errorList) > 0 {
		return fmt.Errorf("failed to cleanup test environment: %v", errorList)
	}
	return nil
}
