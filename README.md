# oomi

Download electricity usage data from the Finnish energy company [Oomi](https://oomi.fi/).

## Running

To run firestore, you need to first create a service account in your GCP project and download 
the credentials as a json file. Then `export GOOGLE_APPLICATION_CREDENTIALS=credentials.json` 
to be able to access your project's firestore. 