from invoke import Collection
from dotenv import load_dotenv

# load .env before importing constants from .helpers:
load_dotenv()


from tasks import zip_volumes, unredact, split_pdfs


ns = Collection()
ns.add_collection(Collection.from_module(zip_volumes))
ns.add_collection(Collection.from_module(unredact))
ns.add_collection(Collection.from_module(split_pdfs))
