/*
Copyright 2021 The Kubernetes Authors.

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

package resources

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Clean-up EventBridge Rules

type EventBridgeRules struct{}

func (EventBridgeRules) MarkAndSweep(opts Options, set *Set) error {
	logger := logrus.WithField("options", opts)
	svc := eventbridge.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))

	input := &eventbridge.ListRulesInput{}

	resp, err := svc.ListRules(input)
	if err != nil {
		return err
	}
	for _, rule := range resp.Rules {
		a := &eventBridgeRule{Account: opts.Account, Region: opts.Region, Name: *rule.Name}
		if !set.Mark(a, nil) {
			continue
		}
		deleteInput := &eventbridge.DeleteRuleInput{
			Name: rule.Name,
		}

		if _, err := svc.DeleteRule(deleteInput); err != nil {
			logger.Warningf("%s: delete failed: %v", *rule.Arn, err)
		}
	}

	return nil
}

func (EventBridgeRules) ListAll(opts Options) (*Set, error) {
	c := eventbridge.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))
	set := NewSet(0)
	input := &eventbridge.ListRulesInput{}

	rules, err := c.ListRules(input)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't describe EventBridge rules for %q in %q", opts.Account, opts.Region)
	}

	now := time.Now()
	for _, rule := range rules.Rules {
		arn := eventBridgeRule{
			Region:  opts.Region,
			Account: opts.Account,
			Name:    aws.StringValue(rule.Name),
		}.ARN()
		set.firstSeen[arn] = now
	}

	return set, nil
}

type eventBridgeRule struct {
	Region  string
	Account string
	Name    string
}

func (e eventBridgeRule) ARN() string {
	return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", e.Region, e.Account, e.Name)
}

func (e eventBridgeRule) ResourceKey() string {
	return e.ARN()
}
