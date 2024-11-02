import aws_cdk as core
import aws_cdk.assertions as assertions

from mid_term.mid_term_stack import MidTermStack

# example tests. To run these tests, uncomment this file along with the example
# resource in mid_term/mid_term_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MidTermStack(app, "mid-term")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
