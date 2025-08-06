#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_state import HDXState
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.econai._version import __version__
from hdx.scraper.econai.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-econai"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: EconAI"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access("26cea7c8-1c85-459e-b3ad-79f9bf5f9799")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with HDXState(
            "pipeline-state-econai",
            tempdir,
            parse_date,
            iso_string_from_datetime,
            configuration,
        ) as state:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=tempdir,
                    save=save,
                    use_saved=use_saved,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                #
                # Steps to generate dataset
                #
                dataset, showcase, last_modified = (
                    pipeline.generate_dataset_and_showcase()
                )
                if last_modified <= state.get():
                    return  # no need to update dataset as no change
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)
                state.set(last_modified)


if __name__ == "__main__":
    facade(
        main,
        #        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
