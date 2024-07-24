import orjson
from rich import print
from rich.console import Console
from typing import Annotated, Optional

import typer

from ftmq_search import __version__
from ftmq.io import smart_read_proxies
from anystore.io import smart_open, smart_stream
from ftmq_search.model import EntityDocument
from ftmq_search.settings import Settings
from ftmq_search.store import get_store

settings = Settings()
cli = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=settings.debug)
console = Console(stderr=True)

state = {"uri": settings.uri, "store": get_store()}


class ErrorHandler:
    def __enter__(self):
        pass

    def __exit__(self, e, msg, _):
        if e is not None:
            if settings.debug:
                raise e
            console.print(f"[red][bold]{e.__name__}[/bold]: {msg}[/red]")
            raise typer.Exit(code=1)


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
        with smart_open(out_uri, "wb") as fh:
            for proxy in smart_read_proxies(in_uri):
                if proxy.schema.is_a("Thing"):
                    data = EntityDocument.from_proxy(proxy)
                    content = data.model_dump_json(by_alias=True)
                    fh.write(content.encode() + b"\n")


@cli.command("index")
def cli_index(in_uri: Annotated[str, typer.Option("-i")] = "-"):
    """
    Index a stream of search documents to a store
    """
    with ErrorHandler():
        for line in smart_stream(in_uri):
            doc = EntityDocument(**orjson.loads(line))
            state["store"].put(doc)
        state["store"].flush()


@cli.command("search")
def cli_search(q: str, out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Simple search against the store
    """
    with ErrorHandler():
        with smart_open(out_uri, "wb") as fh:
            for res in state["store"].search(q):
                content = res.model_dump_json(by_alias=True)
                fh.write(content.encode() + b"\n")


@cli.command("autocomplete")
def cli_autocomplete(q: str, out_uri: Annotated[str, typer.Option("-o")] = "-"):
    """
    Autocomplete based on entities captions
    """
    with ErrorHandler():
        with smart_open(out_uri, "wb") as fh:
            for res in state["store"].autocomplete(q):
                content = res.model_dump_json()
                fh.write(content.encode() + b"\n")
