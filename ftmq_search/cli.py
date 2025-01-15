from typing import Annotated, Iterable, Optional

import typer
from anystore.cli import ErrorHandler
from anystore.io import smart_write, smart_write_json
from anystore.types import SDictGenerator
from anystore.util import model_dump
from pydantic import BaseModel
from rich import print

from ftmq_search import __version__
from ftmq_search.logging import configure_logging
from ftmq_search.logic import index, transform
from ftmq_search.settings import Settings
from ftmq_search.store import get_store
from ftmq_search.store.elastic.mapping import make_mapping

settings = Settings()
cli = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
cli_elastic = typer.Typer(no_args_is_help=True)
cli.add_typer(cli_elastic, name="elastic")

state = {"uri": settings.uri, "store": get_store()}


def serialize(items: Iterable[BaseModel]) -> SDictGenerator:
    for item in items:
        yield model_dump(item)


@cli.callback(invoke_without_command=True)
def cli_ftmqs(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False,
    uri: Annotated[
        Optional[str], typer.Option(..., help="Store base uri")
    ] = settings.uri,
):
    if version:
        print(__version__)
        raise typer.Exit()
    configure_logging()
    state["uri"] = uri or settings.uri
    state["store"] = get_store(uri=state["uri"])


@cli.command("transform")
def cli_transform(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Create search documents from a stream of followthemoney entities
    """
    with ErrorHandler():
        transform(in_uri, out_uri)


@cli.command("index")
def cli_index(in_uri: Annotated[str, typer.Option("-i")] = "-"):
    """
    Index a stream of search documents to a store
    """
    with ErrorHandler():
        index(in_uri, state["store"])


@cli.command("search")
def cli_search(q: str, out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Simple search against the store
    """
    with ErrorHandler():
        res = serialize(state["store"].search(q))
        smart_write_json(out_uri, res)


@cli.command("autocomplete")
def cli_autocomplete(q: str, out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Autocomplete based on entities captions
    """
    with ErrorHandler():
        res = serialize(state["store"].autocomplete(q))
        smart_write_json(out_uri, res)


@cli_elastic.command("init")
def cli_elastic_init():
    """
    Setup elasticsearch index
    """
    with ErrorHandler():
        state["store"].init()


@cli_elastic.command("mapping")
def cli_elastic_mapping(out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Print elasticsearch mapping
    """
    with ErrorHandler():
        content = make_mapping()
        smart_write_json(out_uri, [content])


@cli_elastic.command("logstash")
def cli_elastic_logstash(out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Print logstash config
    """
    with ErrorHandler():
        content = state["store"].make_logstash()
        smart_write(out_uri, content)
