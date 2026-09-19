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

package controllerutils

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

const testConditionType = "TestReady"

func TestIsAlreadyReconciled(t *testing.T) {
	tests := []struct {
		name                      string
		generation                int64
		lastAppliedGeneration     int64
		conditions                []metav1.Condition
		conditionType             string
		expectedAlreadyReconciled bool
	}{
		{
			name:                  "generation matches and condition is true",
			generation:            2,
			lastAppliedGeneration: 2,
			conditions: []metav1.Condition{
				{Type: testConditionType, Status: metav1.ConditionTrue},
			},
			conditionType:             testConditionType,
			expectedAlreadyReconciled: true,
		},
		{
			name:                  "generation does not match",
			generation:            3,
			lastAppliedGeneration: 2,
			conditions: []metav1.Condition{
				{Type: testConditionType, Status: metav1.ConditionTrue},
			},
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
		{
			name:                  "generation matches but condition is false",
			generation:            2,
			lastAppliedGeneration: 2,
			conditions: []metav1.Condition{
				{Type: testConditionType, Status: metav1.ConditionFalse},
			},
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
		{
			name:                  "generation matches but condition is unknown",
			generation:            2,
			lastAppliedGeneration: 2,
			conditions: []metav1.Condition{
				{Type: testConditionType, Status: metav1.ConditionUnknown},
			},
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
		{
			name:                      "generation matches but no conditions",
			generation:                1,
			lastAppliedGeneration:     1,
			conditions:                nil,
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
		{
			name:                  "generation matches but wrong condition type",
			generation:            1,
			lastAppliedGeneration: 1,
			conditions: []metav1.Condition{
				{Type: "OtherCondition", Status: metav1.ConditionTrue},
			},
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
		{
			name:                      "brand new object with zero generation",
			generation:                1,
			lastAppliedGeneration:     0,
			conditions:                nil,
			conditionType:             testConditionType,
			expectedAlreadyReconciled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAlreadyReconciled(tt.generation, tt.lastAppliedGeneration, tt.conditions, tt.conditionType)
			if result != tt.expectedAlreadyReconciled {
				t.Errorf("IsAlreadyReconciled() = %v, want %v", result, tt.expectedAlreadyReconciled)
			}
		})
	}
}

func TestReconcileNeededPredicate_Create(t *testing.T) {
	predicate := ReconcileNeededPredicate{ConditionType: operatorv1alpha1.SourceCrawlerCondition}

	tests := []struct {
		name           string
		object         *operatorv1alpha1.SourceCrawler
		expectedResult bool
	}{
		{
			name: "new CR that has never been reconciled — should reconcile",
			object: &operatorv1alpha1.SourceCrawler{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Status: operatorv1alpha1.SourceCrawlerStatus{
					LastAppliedGeneration: 0,
				},
			},
			expectedResult: true,
		},
		{
			name: "already reconciled CR — should skip (restart re-list)",
			object: &operatorv1alpha1.SourceCrawler{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Status: operatorv1alpha1.SourceCrawlerStatus{
					LastAppliedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: operatorv1alpha1.SourceCrawlerCondition, Status: metav1.ConditionTrue},
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "spec changed since last reconcile — should reconcile",
			object: &operatorv1alpha1.SourceCrawler{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status: operatorv1alpha1.SourceCrawlerStatus{
					LastAppliedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: operatorv1alpha1.SourceCrawlerCondition, Status: metav1.ConditionTrue},
					},
				},
			},
			expectedResult: true,
		},
		{
			name: "last reconcile failed — should reconcile",
			object: &operatorv1alpha1.SourceCrawler{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Status: operatorv1alpha1.SourceCrawlerStatus{
					LastAppliedGeneration: 1,
					Conditions: []metav1.Condition{
						{Type: operatorv1alpha1.SourceCrawlerCondition, Status: metav1.ConditionFalse},
					},
				},
			},
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event.CreateEvent{Object: tt.object}
			result := predicate.Create(e)
			if result != tt.expectedResult {
				t.Errorf("ReconcileNeededPredicate.Create() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestReconcileNeededPredicate_OtherEvents(t *testing.T) {
	predicate := ReconcileNeededPredicate{ConditionType: testConditionType}

	if !predicate.Update(event.UpdateEvent{}) {
		t.Error("Update() should always return true")
	}
	if !predicate.Delete(event.DeleteEvent{}) {
		t.Error("Delete() should always return true")
	}
	if !predicate.Generic(event.GenericEvent{}) {
		t.Error("Generic() should always return true")
	}
}
