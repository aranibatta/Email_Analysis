# Enron Email Analysis


This is an analysis of the enron email dataset provided by [Berkeley](http://bailando.sims.berkeley.edu/enron_email.html).

### Questions:
1. How many emails did each person receive each day? 
2. Let's label an email as "direct" if there is exactly one recipient and "broadcast" if it has multiple recipients. Identify the person (or people) who received the largest number of direct emails and the person (or people) who sent the largest number of broadcast emails.
3. Find the five emails with the fastest response times. (A response is defined as a message from one of the recipients to the original sender whose subject line contains all of the words from the subject of the original email, and the response time should be measured as the difference between when the original email was sent and when the response was sent.)


### Steps to run:
1. Spin up MySQL and use configuration details in ddl.yaml
2. Run "python main.py"

The solutions to the questions will be printed in console. 

Shoot questions to [Ron](mailto:aranibatta@berkeley.edu)
