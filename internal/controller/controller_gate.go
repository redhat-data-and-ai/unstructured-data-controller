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

type disabledControllers map[string]bool

type controllerGate struct {
	disabledControllers disabledControllers
}

var (
	sharedControllerGate *controllerGate
)

func getSharedControllerGate() *controllerGate {
	if sharedControllerGate == nil {
		sharedControllerGate = &controllerGate{}
		sharedControllerGate.init()
	}
	return sharedControllerGate
}

// init controller gate
func (cg *controllerGate) init() {
	cg.disabledControllers = make(disabledControllers)
}

// disable a list of controllers by name
func (cg *controllerGate) disableControllers(ctrlNames ...string) {
	for _, ctrlName := range ctrlNames {
		cg.disabledControllers[ctrlName] = true
	}
}

// enable all controllers
func (cg *controllerGate) enableAllControllers() {
	cg.init()
}

// setDisabledControllers sets the controller gate to default state (enable all controllers)
// and then disables the given list of controllers.
func (cg *controllerGate) setDisabledControllers(ctrlNames ...string) {
	cg.enableAllControllers()
	cg.disableControllers(ctrlNames...)
}

func (cg *controllerGate) controllerInDisabledSet(ctrlName string) bool {
	return cg.disabledControllers[ctrlName]
}

// isControllerDisabled returns true if the given controller name is in the disabled set.
func isControllerDisabled(ctrlName string) bool {
	return getSharedControllerGate().controllerInDisabledSet(ctrlName)
}
