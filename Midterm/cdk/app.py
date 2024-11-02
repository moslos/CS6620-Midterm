#!/usr/bin/env python3
import aws_cdk as cdk
from storage_stack import StorageStack
from replicator_stack import ReplicatorStack
from cleaner_stack import CleanerStack
from aws_cdk_lib import App

app = cdk.App()

# Instantiate the storage stack
storage_stack = StorageStack(app, "StorageStack")

# Instantiate the Replicator stack, passing the storage stack
replicator_stack = ReplicatorStack(app, "ReplicatorStack", storage_stack)

# Instantiate the Cleaner stack, passing the storage stack
cleaner_stack = CleanerStack(app, "CleanerStack", storage_stack)

app.synth()
