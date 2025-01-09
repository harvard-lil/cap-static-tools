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
          create-index-html.create-html              Creates and uploads index.html pages to the static bucket.
          sync-static-bucket.pdf-paths               Creates file path pairs to copy pdf files from s3 to r2 cap-static bucket.
          sync-static-bucket.tar-paths               Creates file path pairs to copy tar files from s3 to r2 cap-static bucket.
          unredact.pdf-paths                         Creates file path pairs to copy unredacted pdfs from S3 to r2 unredacted bucket.
          unredact.tar-paths                         Creates file path pairs to copy unredacted tars to r2 unredacted bucket.
          unredact.unredact-volumes                  Creates file path pairs to copy unredacted volume files from r2 unredacted bucket to static bucket.
          unredact.update-volume-fields              Updates the redacted and last_updated fields in top level and reporter level metadata files.
          
          unredact.add-last-updated-field            Populates VolumesMetadata.json files with last_updated field.
          zip-volumes.zip-volumes (zip-volumes)      Downloads data for each volume from r2, zips, and uploads.
          

Use `inv <command name>` to run a command.

Use `inv -h <command name>` to see help for a command.

### split-pdfs command

'split-pdfs' allows volume PDFs to be split into individual case PDFs. It can
process all volumes and jurisdictions as they are in metadata files, or it can
be limited to specific params.

Options:

- `--reporter`: Specify a reporter slug to process only volumes from that
  reporter.
- `--volume`: Specify a volume slug (along with `--reporter`) to process
  only a specific volume from that reporter.
- `--publication-year`: Specify a year to process only volumes published in that
  year.

Examples:

- Process all volumes: `inv split-pdfs.split-pdfs`
- Process volumes from a specific reporter: `inv split-pdfs.split-pdfs --reporter cal`
- Process volumes from a specific volume: `inv split-pdfs.split-pdfs --reporter pa --volume 81-12`
- Process volumes from a specific year: `inv split-pdfs.split-pdfs --publication-year 2023`

## Develop

Add new tasks to a file within `tasks/`, grouped by subject, import them in
`tasks/__init__.py`, and add them to the collection `ns`.

Use Poetry to manage dependencies; for instance, add packages with
`poetry add <package>`, but make sure to run `poetry export -o
requirements.txt` to keep the non-Poetry requirements file up to date.

## Test

Add tests for each file in tasks/ to a file within tests/.

