from invoke import Collection
from dotenv import load_dotenv

# load .env before importing constants from .helpers:
load_dotenv()


from tasks import zip_volumes, unredact, split_pdfs, sync_static_bucket, create_index_html


ns = Collection()
ns.add_collection(Collection.from_module(zip_volumes))
ns.add_collection(Collection.from_module(unredact))
ns.add_collection(Collection.from_module(split_pdfs))
ns.add_collection(Collection.from_module(sync_static_bucket))
ns.add_collection(Collection.from_module(create_index_html))