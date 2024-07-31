
# cap-static-tools

Commands to work on the files served at https://static.case.law/

## Install

Make a virtualenv of your choice, then:

    pip install -r requirements.txt

## Use

Use `inv` to run tasks defined in tasks.py:

    $ inv -l
      Available tasks:
        
          unredact.pdf-paths                         Create file path pairs to copy unredacted pdfs from S3 to unredacted r2 bucket.
          unredact.tar-paths                         Create file path pairs to copy unredacted tars to unredacted r2 bucket.
          unredact.update-redacted-field-of-volume   Update the redacted flags in top level and reporter level metadata files
          unredact.volume-paths                      Create file path pairs to copy unredacted volume files from r2 unredacted bucket to static bucket.
          zip-volumes.zip-volumes (zip-volumes)      Download data for each volume from R2, zip, and upload.

Use `inv <command name>` to run a command.

Use `inv -h <command name>` to see help for a command.

## Develop

Add new tasks to a file within tasks/, grouped by subject, and import them in tasks/__init__.py.

## Test

Add tests for each file in tasks/ to a file within tests/.

## Legacy

We still have some legacy tasks that were written for lambda. These should be migrated
to tasks.py before running:

### legacy/pdf folder:

- **`copy-archive-data-to-r2-trigger`** lambda prepares the data and triggers the file upload lambda in batches. Job expects the environment variables to be set, and its execution role to have invoke access to the upload lambda.
- **`copy-archive-data-to-r2`** lambda receives the event information and makes file upload calls to R2 with S3 objects' content. Environment variables are needed to be set.


### legacy/tar folder:

- **`copy-tar-files-to-r2-trigger`** lambda prepares the data and triggers the file upload lambda in batches. Job expects the environment variables to be set, and its execution role to have invoke access to the upload lambda.
- **`copy-tar-files-to-r2`** lambda receives the event information and makes file upload calls to R2 with S3 objects' content. Environment variables are needed to be set.


### legacy/index.html folder:

- **`create-index-html-trigger`** lambda uploads the first and second level index htmls to R2, and triggers the create-index-html in batches for third and fourth levels. Job expects the environment variables to be set, its execution role to have invoke access to the triggered lambda, and the AWSSDKPandas-Python312-Arm64 layer to be attached.
- **`create-index-html`** lambda receives the event information, and creates and uploads the third and fourth level htmls to R2. Environment variables are needed to be set, and the AWSSDKPandas-Python312-Arm64 layer needs to be attached to the lambda.
