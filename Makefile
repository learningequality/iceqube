pubsubemulator:
	gcloud beta emulators pubsub start

simpletest:
	pip install --upgrade . && pytest
