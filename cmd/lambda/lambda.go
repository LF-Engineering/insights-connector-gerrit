package main

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecs"
	"log"
	"os"
)


func HandleRequest(ctx context.Context) {
	log.Println("lambda invoked")
	config := &aws.Config{
		Region:           aws.String(os.Getenv("AWS_REGION")),
	}
	mysession := session.Must(session.NewSession(config))
	svc := ecs.New(mysession)
	runTaskInp := &ecs.RunTaskInput{

		Cluster:                  aws.String(os.Getenv("ECS_CLUSTER_NAME")),
		Count:                    aws.Int64(1),
		LaunchType:               aws.String("FARGATE"),
		NetworkConfiguration:    & ecs.NetworkConfiguration{AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
			AssignPublicIp: aws.String("DISABLED"),
			SecurityGroups: nil,
			Subnets:        []*string{aws.String(os.Getenv("SUBNET_ID"))},
		}},
		Overrides:               & ecs.TaskOverride{
			ContainerOverrides:        []*ecs.ContainerOverride{{
				Name:                 aws.String("insights-gerrit"),
				Environment:
          []*ecs.KeyValuePair{
            &ecs.KeyValuePair{
              Name: aws.String("GERRIT_URL"),
              Value: aws.String("GERRIT_URL"),
            },
            &ecs.KeyValuePair{
              Name: aws.String("GERRIT_PROJECT"),
              Value: aws.String("GERRIT_PROJECT"),
            },
            &ecs.KeyValuePair{
              Name: aws.String("GERRIT_PROJECT_FILTER"),
              Value: aws.String("GERRIT_PROJECT_FILTER"),
            },
            &ecs.KeyValuePair{
              Name: aws.String("ES_URL"),
              Value: aws.String(os.Getenv("ES_URL")),
            },
          },
            &ecs.KeyValuePair{
              Name: aws.String("STAGE"),
              Value: aws.String(os.Getenv("STAGE")),
            },
			},
		}},
		TaskDefinition: aws.String(os.Getenv("TASK_DEFINITION")),
	}
	log.Println("Input Prepared")
	output, err := svc.RunTask(runTaskInp)
	if err != nil {
		log.Fatal("couldn't spawn the ecs task", err.Error())
	}
	log.Println(output)
}

func main() {
	lambda.Start(HandleRequest)
}
