#!/usr/bin/env python3.9

from aws_cdk import App, Stack, Environment
from mid_term.mid_term_stack import MidTermStack
from mid_term.replicator_lambda_stack import ReplicatorLambdaStack
from mid_term.cleaner_lambda_stack import CleanerStack

# Initialize the CDK application
app = App()

# Define the environment for deployment
env_us_east_1 = Environment(region="us-east-1")

# Deploy the MidTerm stack
MidTermStack(
    app,
    "MidTermStack",
    env=env_us_east_1
)

# Deploy the Replicator Lambda stack
ReplicatorLambdaStack(
    app,
    "ReplicatorLambdaStack",
    env=env_us_east_1
)

# Deploy the Cleaner Lambda stack
CleanerStack(
    app,
    "CleanerStack",
    env=env_us_east_1
)

# Synthesize the application
app.synth()
