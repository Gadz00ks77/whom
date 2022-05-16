# WHOM
## An Identity Service for Business Domain Objects

We've all been there; System X calls a policy "Y" but System Z calls the same policy "Trevor". Unpick this A-Grade nonsense with a serverless service that will let your systems all agree it's called "bc54a657-f6c4-4f28-bfe0-a06bd50bf96e" (Ooooh SNAPPY!).

(Oh, and if you've not "been there", congratulations you've never worked in Corporate!)

## Requirements
- An AWS Account
- A Business Problem where your systems aren't integrated!

 ## TODO

 - Review GSI calls for eventual consistency potential issues
 -- whom-fetch-results; no change required as the chunks must be marked complete.
 -- whom-ticket-process-chunk; change as per TO DO in code
 -- whom-ticket-send-to-queue; change as per TO DO in code
 -- whom-ticket-start-chunk-process; no change required if "send to queue" fix above is accounted for. 