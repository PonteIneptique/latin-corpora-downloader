import requests
import zipfile
import csv
import os
import shutil
from io import BytesIO
import glob
import logging
from capitains_nautilus.cts.resolver import NautilusCTSResolver
import click
from werkzeug.contrib.cache import FileSystemCache


def download_corpus(tgt, corpus_name, corpus_version):
    """ Download a corpus

    :param tgt: Directory where to download
    :param corpus_name: Corpus Name
    :param corpus_version: Corpus version
    :return: Status
    :rtype: bool
    """
    target_dir = tgt+"/"+corpus_name.replace("/", "_")
    if os.path.isdir(target_dir):
        shutil.rmtree(target_dir)
    print("Starting download")
    webfile = requests.get("https://github.com/{name}/archive/{version}.zip".format(
        name=corpus_name, version=corpus_version
    ))
    print("Starting Unzipping")
    with zipfile.ZipFile(BytesIO(webfile.content)) as z:
        z.extractall(target_dir)
    print("Done")
    return True, target_dir


def download_corpora(src="corpora.csv", tgt="data/", force=False, cache=None):
    if src is None:
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpora.csv")
    if tgt is None:
        tgt = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    with open(src) as src_file:
        corpora = [corpus for corpus in csv.DictReader(src_file, delimiter=";")]
        new_corpora = []
        for corpus in corpora:
            if corpus["Current"] == corpus["Version"] and force is not True:
                print("{} stays on version {}".format(corpus["Name"], corpus["Current"]))
            else:
                print("{}'s version is {}. Downloading {}".format(corpus["Name"], corpus["Current"], corpus["Version"]))
                status, path = download_corpus(tgt, corpus["Name"], corpus["Version"])
                if status is True:
                    corpus["Current"] = corpus["Version"]
                    print("Cleaning up the corpus")
                    clean_up_corpora(path)
            new_corpora.append({k: v for k, v in corpus.items()})

    # Update the corpus
    with open(src, "w") as src_file:
        writer = csv.DictWriter(src_file, delimiter=";", fieldnames=["Name", "Version", "Current"])
        writer.writeheader()
        writer.writerows(new_corpora)

    if cache:
        resolver = make_resolver(
            glob.glob(os.path.join(tgt, "**", "**")),
            cache_directory=cache
        )
        print("Parsing to cache")
        resolver.parse()


def clean_up_corpora(src):
    resolver = make_resolver(glob.glob(src+"/**"))
    translations = [x.path for x in resolver.getMetadata().readableDescendants if x.lang != "lat"]
    for trans in translations:
        os.remove(trans)
    print("Removed {} text(s) not in Latin".format(len(translations)))
    print("Kept {} text(s) in Latin".format(
        len([x for x in resolver.getMetadata().readableDescendants if x.lang == "lat"]))
    )


def make_resolver(directories=None, cache_directory=None):
    """ Generate the CapiTainS Resolver and add metadata to it
    """
    if directories is None:
        directories = glob.glob("data/raw/corpora/**/**")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.CRITICAL)

    kwargs = dict(
        resource=directories,
        logger=logger
    )
    if cache_directory:
        kwargs["cache"] = FileSystemCache(cache_directory)
        print("Clearing cache")
        kwargs["cache"].clear()

    resolver = NautilusCTSResolver(**kwargs)
    return resolver


@click.command("download")
@click.argument('target', type=click.Path(exists=False))
@click.option("--cache", type=click.Path(exists=False))
@click.option("--source", type=click.Path(exists=True))
def download_command(target, cache=None, source=None):
    download_corpora(src=source, tgt=target, cache=cache)


if __name__ == "__main__":
    download_command()
