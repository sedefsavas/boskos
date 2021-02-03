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
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// SQS queues: https://docs.aws.amazon.com/sdk-for-go/api/service/sqs

type SQSQueues struct{}

func (SQSQueues) MarkAndSweep(opts Options, set *Set) error {
	logger := logrus.WithField("options", opts)
	svc := sqs.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))

	input := &sqs.ListQueuesInput{}

	var toDelete []*sqsQueue // Paged call, defer deletion until we have the whole list.

	pageFunc := func(page *sqs.ListQueuesOutput, _ bool) bool {
		for _, url := range page.QueueUrls {
			attrInput := &sqs.GetQueueAttributesInput{
				AttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
				QueueUrl:       url,
			}
			attr, err := svc.GetQueueAttributes(attrInput)
			if err != nil {
				return false
			}
			q := &sqsQueue{
				Account:  opts.Account,
				Region:   opts.Region,
				Name:     *attr.Attributes[sqs.QueueAttributeNameQueueArn],
				QueueURL: *url,
			}

			unitTimestamp, _ := strconv.ParseInt(*attr.Attributes[sqs.QueueAttributeNameCreatedTimestamp], 10, 64)
			creationTime := time.Unix(unitTimestamp, 0)
			if set.Mark(q, &creationTime) {
				logger.Warningf("%s: deleting %T: %s", q.ARN(), url, q.Name)
				if !opts.DryRun {
					toDelete = append(toDelete, q)
				}
			}
		}
		return true
	}

	if err := svc.ListQueuesPages(input, pageFunc); err != nil {
		return err
	}
	for _, q := range toDelete {
		_, err := svc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: aws.String(q.QueueURL)})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
					continue
				}
			}
			logger.Warningf("%s: delete failed: %v", q.ARN(), err)
		}
	}
	return nil
}

func (SQSQueues) ListAll(opts Options) (*Set, error) {
	svc := sqs.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))
	set := NewSet(0)
	input := &sqs.ListQueuesInput{}

	err := svc.ListQueuesPages(input, func(queues *sqs.ListQueuesOutput, _ bool) bool {
		for _, url := range queues.QueueUrls {
			attrInput := &sqs.GetQueueAttributesInput{
				AttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
				QueueUrl:       url,
			}
			attr, err := svc.GetQueueAttributes(attrInput)
			if err != nil {
				return false
			}
			now := time.Now()
			arn := sqsQueue{
				Account:  opts.Account,
				Region:   opts.Region,
				Name:     *attr.Attributes[sqs.QueueAttributeNameQueueArn],
				QueueURL: *url,
			}.ARN()
			set.firstSeen[arn] = now
		}
		return true
	})
	return set, errors.Wrapf(err, "couldn't describe sqs queues for %q in %q", opts.Account, opts.Region)
}

type sqsQueue struct {
	Account  string
	Region   string
	Name     string
	QueueURL string
}

func (i sqsQueue) ARN() string {
	// arn:aws:sqs:us-west-1:111111111111:name
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", i.Region, i.Account, i.Name)
}

func (i sqsQueue) ResourceKey() string {
	return i.ARN()
}
