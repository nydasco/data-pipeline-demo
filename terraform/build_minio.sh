#!/bin/bash

docker run -it -v $PWD:$PWD -w $PWD hashicorp/terraform:latest init

#docker run -it -v $PWD:$PWD -w $PWD hashicorp/terraform:latest plan

docker run -it -v $PWD:$PWD -w $PWD hashicorp/terraform:latest apply -auto-approve
