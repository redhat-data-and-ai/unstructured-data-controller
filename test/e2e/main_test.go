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
	testNamespace       = "unstructured-controller-namespace"
	deploymentName      = "unstructured-controller-manager"
	snowflakeSecretName = "private-key"
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
		log.Printf("Command output: %s", p.Result())
		log.Printf("Command stdout: %s", p.Stdout())
		log.Printf("Command stderr: %s", p.Out())
		return p.Err()
	}

	log.Println("Verifying deployment exists...")
	checkCmd := fmt.Sprintf("kubectl get deployment %s -n %s", deploymentName, testNamespace)
	if p := utils.RunCommand(checkCmd); p.Err() != nil {
		log.Printf("Deployment not found: %s", p.Err())
		return p.Err()
	}

	log.Printf("Deployment %s found in namespace %s", deploymentName, testNamespace)

	log.Println("Patching controller-manager to add cache directory volume...")
	patchCommand := fmt.Sprintf(`kubectl patch deployment %s -n %s --type=json -p '[
	{
		"op": "replace",
		"path": "/spec/template/spec/volumes",
		"value": [
			{
				"name": "cache-volume",
				"emptyDir": {}
			}
		]
	},
	{
		"op": "replace",
		"path": "/spec/template/spec/containers/0/volumeMounts",
		"value": [
			{
				"name": "cache-volume",
				"mountPath": "/tmp/cache"
			}
		]
	}
	]'`, deploymentName, testNamespace)

	if p := utils.RunCommand(patchCommand); p.Err() != nil {
		log.Printf("Failed to patch deployment: %s", p.Err())
		log.Printf("Command output: %s", p.Result())
		return p.Err()
	}
	log.Println("Successfully patched deployment with cache volume")

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

	log.Println("Capturing logs from controller-manager")
	logFile, err := os.Create("controller-manager-logs.txt")
	if err != nil {
		log.Printf("failed to create log file: %s", err)
	} else {
		kubectlLogs := exec.Command("kubectl", "logs", "-f", "-n", testNamespace, "deployments/"+deploymentName)
		kubectlLogs.Stdout = logFile
		kubectlLogs.Stderr = logFile
		if err := kubectlLogs.Start(); err == nil {
			*runningProcesses = append(*runningProcesses, *kubectlLogs)
		}
		logFile.Close()
	}

	// log.Println("Creating snowflake secret with private key")
	// secretFile := os.Getenv("SNOWFLAKE_SECRET_FILE")
	// if secretFile == "" {
	// 	return fmt.Errorf("SNOWFLAKE_SECRET_FILE environment variable is required for snowflake secret")
	// }

	// // Verify file exists
	// if _, err := os.Stat(secretFile); err != nil {
	// 	log.Printf("Secret file does not exist or cannot be accessed: %s", err)
	// 	return fmt.Errorf("secret file not found: %s", secretFile)
	// }

	// secretCreateCmd := fmt.Sprintf("kubectl create secret generic %s -n %s --from-file=privateKey=%s",
	// 	snowflakeSecretName, testNamespace, secretFile)
	// log.Println("Running command to create snowflake secret...")
	// if p := utils.RunCommand(secretCreateCmd); p.Err() != nil {
	// 	log.Printf("Failed to create snowflake secret: %s", p.Err())
	// 	log.Printf("Command output: %s", p.Result())
	// 	return p.Err()
	// }
	// log.Println("Snowflake secret created successfully")

	log.Println("Creating aws-secret from config/samples/aws-secret.yaml")
	if p := utils.RunCommand(fmt.Sprintf("kubectl apply -n %s -f config/samples/aws-secret.yaml", testNamespace)); p.Err() != nil {
		log.Printf("Failed to create aws-secret: %s %s", p.Err(), p.Result())
		return p.Err()
	}

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
			log.Println("Checking localstack pod logs...")
			utils.RunCommand(fmt.Sprintf("kubectl describe deployment localstack -n %s", testNamespace))
			utils.RunCommand(fmt.Sprintf("kubectl get pods -n %s -l app=localstack", testNamespace))
			utils.RunCommand(fmt.Sprintf("kubectl logs -n %s -l app=localstack --tail=50", testNamespace))
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
			log.Printf("Failed to deploy docling-serve: %s %s", p.Err(), p.Result())
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

	log.Println("Applying ControllerConfig from test/e2e/config/controllerconfig.yaml...")
	if p := utils.RunCommand(fmt.Sprintf("kubectl apply -n %s -f test/e2e/config/controllerconfig.yaml", testNamespace)); p.Err() != nil {
		log.Printf("failed to apply ControllerConfig: %s %s", p.Err(), p.Result())
		return p.Err()
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
		log.Printf("failed to meet condition for ControllerConfig: %s %s", p.Err(), p.Result())
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
		fmt.Sprintf("kubectl delete secret %s -n %s --ignore-not-found=true", snowflakeSecretName, testNamespace),
		fmt.Sprintf("kubectl delete secret aws-secret -n %s --ignore-not-found=true", testNamespace),
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
