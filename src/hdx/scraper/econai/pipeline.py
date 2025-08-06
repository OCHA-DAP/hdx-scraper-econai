#!/usr/bin/python
"""EconAI scraper"""

import logging
from datetime import datetime, timezone
from typing import Tuple

from hdx.api.configuration import Configuration
from hdx.api.utilities.date_helper import DateHelper
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    get_datetime_from_timestamp,
    parse_date_range,
)
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    type_mapping = {
        "armedconf": "Armed Conflict",
        "anyviolence": "Any Violence",
        "lnbest": "Violence Intensity",
    }

    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._base_url = configuration["base_url"]
        latest_path = configuration["latest_path"]
        self._latest_url = f"{self._base_url}{latest_path}"
        self._retriever = retriever
        self._tempdir = tempdir
        self._start_date = default_enddate
        self._end_date = default_date

    def add_resources(self, dataset: Dataset) -> datetime:
        json = self._retriever.download_json(self._latest_url)
        codebook_resource = None
        latest_last_modified = default_date
        for file in json:
            filename = file["name"]
            last_modified = get_datetime_from_timestamp(
                file["updatedOn"], timezone=timezone.utc
            )
            created_date = get_datetime_from_timestamp(
                file["createdOn"], timezone=timezone.utc
            )
            created = DateHelper.get_hdx_date(
                created_date, ignore_timeinfo=False, include_microseconds=True
            )
            url = file["url"]
            path = self._retriever.download_file(f"{self._base_url}{url}")
            if "codebook" in filename:
                is_codebook = True
                description = "Codebook"
            else:
                is_codebook = False
                timeframe = int(filename[-6:-4])
                filetype_short = filename[21:-7]
                filetype = self.type_mapping[filetype_short]
                description = f"{filetype} over {timeframe} months"
            resource = Resource(
                {"name": filename, "description": description, "created": created}
            )
            resource.set_format(filename[-3:])
            resource.set_file_to_upload(path)
            resource.set_date_data_updated(last_modified)
            if last_modified > latest_last_modified:
                latest_last_modified = last_modified
            if is_codebook:
                codebook_resource = resource
                continue
            dataset.add_update_resource(resource)
            headers, iterator = self._retriever.downloader.get_tabular_rows(
                path, dict_form=True
            )
            for row in iterator:
                period = row["period"]
                start_date, end_date = parse_date_range(f"{period[:4]}-{period[-2:]}")
                if start_date < self._start_date:
                    self._start_date = start_date
                if end_date > self._end_date:
                    self._end_date = end_date
        if codebook_resource:
            dataset.add_update_resource(codebook_resource)
        return latest_last_modified

    def generate_dataset_and_showcase(self) -> Tuple[Dataset, Showcase, datetime]:
        # Dataset info
        title = "EconAI Conflict Forecast"
        name = slugify(title)
        dataset = Dataset(
            {
                "name": name,
                "title": title,
            }
        )

        tag = "conflict-violence"
        dataset.add_tag(tag)
        dataset.set_subnational(False)
        dataset.add_other_location("world")

        last_modified = self.add_resources(dataset)
        dataset.set_time_period(self._start_date, self._end_date)

        showcase = Showcase(
            {
                "name": f"{name}-showcase",
                "title": title,
                "notes": "Conflict Prevention Gains",
                "url": "https://conflictforecast.org/prevention-gains",
                "image_url": "https://raw.githubusercontent.com/mcarans/hdx-scraper-econai/main/gridcells.png",  # FIXME
            }
        )
        showcase.add_tag(tag)

        return dataset, showcase, last_modified
