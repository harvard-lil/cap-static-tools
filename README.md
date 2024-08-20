# cap-static-tools

Commands to work on the files served at https://static.case.law/

## Install

Make a virtualenv of your choice, then:

    pip install -r requirements.txt

or use [Poetry](https://python-poetry.org/); run

    poetry install

to set up the environment, then preface the following `invoke`
commands with `poetry run `.

## Use

Use `inv` to run tasks defined in `tasks/`:

    $ inv -l
      Available tasks:
        
          split-pdfs.split-pdfs                      Split PDFs into individual case files for all jurisdictions or a specific reporter.
          unredact.pdf-paths                         Create file path pairs to copy unredacted pdfs from S3 to unredacted r2 bucket.
          unredact.tar-paths                         Create file path pairs to copy unredacted tars to unredacted r2 bucket.
          unredact.update-redacted-field-of-volume   Update the redacted flags in top level and reporter level metadata files
          unredact.volume-paths                      Create file path pairs to copy unredacted volume files from r2 unredacted bucket to static bucket.
          zip-volumes.zip-volumes (zip-volumes)      Download data for each volume from R2, zip, and upload.

Use `inv <command name>` to run a command.

Use `inv -h <command name>` to see help for a command.

### split-pdfs command

'split-pdfs' allows volume PDFs to be split into individual case PDFs. It can
process all volumes and jurisdictions as they are in metadata files, or it can
be limited to specific params.

Options:

- `--reporter`: Specify a reporter slug to process only volumes from that
  reporter.
- `--publication-year`: Specify a year to process only volumes published in that
  year.

Examples:

- Process all volumes: `inv split-pdfs.split-pdfs`
- Process volumes from a specific reporter: `inv split-pdfs.split-pdfs --reporter cal`
- Process volumes from a specific year: `inv split-pdfs.split-pdfs --publication-year 2023`

## Develop

Add new tasks to a file within `tasks/`, grouped by subject, import them in
`tasks/__init__.py`, and add them to the collection `ns`.

Use Poetry to manage dependencies; for instance, add packages with
`poetry add <package>`, but make sure to run `poetry export -o
requirements.txt` to keep the non-Poetry requirements file up to date.

## Test

Add tests for each file in tasks/ to a file within tests/.

## Legacy

We still have some legacy tasks that were written for lambda. These should be
migrated to tasks.py before running:

### legacy/pdf folder:

- **`copy-archive-data-to-r2-trigger`** lambda prepares the data and triggers
  the file upload lambda in batches. Job expects the environment variables to be
  set, and its execution role to have invoke access to the upload lambda.
- **`copy-archive-data-to-r2`** lambda receives the event information and makes
  file upload calls to R2 with S3 objects' content. Environment variables are
  needed to be set.

### legacy/tar folder:

- **`copy-tar-files-to-r2-trigger`** lambda prepares the data and triggers the
  file upload lambda in batches. Job expects the environment variables to be
  set, and its execution role to have invoke access to the upload lambda.
- **`copy-tar-files-to-r2`** lambda receives the event information and makes
  file upload calls to R2 with S3 objects' content. Environment variables are
  needed to be set.

### legacy/index.html folder:

- **`create-index-html-trigger`** lambda uploads the first and second level
  index htmls to R2, and triggers the create-index-html in batches for third and
  fourth levels. Job expects the environment variables to be set, its execution
  role to have invoke access to the triggered lambda, and the
  AWSSDKPandas-Python312-Arm64 layer to be attached.
- **`create-index-html`** lambda receives the event information, and creates and
  uploads the third and fourth level htmls to R2. Environment variables are
  needed to be set, and the AWSSDKPandas-Python312-Arm64 layer needs to be
  attached to the lambda.
