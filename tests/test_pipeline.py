from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.econai.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestEconAI",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                dataset, showcase, last_modified = (
                    pipeline.generate_dataset_and_showcase()
                )
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "data_update_frequency": 30,
                    "dataset_date": "[2010-01-01T00:00:00 TO 2025-08-31T23:59:59]",
                    "dataset_source": "EconAI",
                    "groups": [{"name": "world"}],
                    "license_id": "cc-by",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Other",
                    "methodology_other": "For more information, see [the codebook available at ConflictForecast](https://conflictforecast.org/downloads)\n",
                    "name": "econai-conflict-forecast",
                    "notes": "Armed conflict - or the fear of it - is one of the central problems we face in our societies. At ConflictForecast, we want to contribute to peacebuilding by making the allocation of public resources and attention smarter and more efficient.\n\n"
                    "Our goal is to change this approach in the long term. We specialize in conflict forecasting, prevention, and decision support. Our forecasting methodology is explicitly designed to detect new, subtle signals of conflict risk in countries not currently at war.\n",
                    "owner_org": "26cea7c8-1c85-459e-b3ad-79f9bf5f9799",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "forecasting",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "EconAI Conflict Forecast",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "created": "2025-09-03T13:02:26.805000",
                        "description": "Grid risk",
                        "format": "csv",
                        "last_modified": "2025-09-03T13:02:26.805000",
                        "name": "conflictforecast_grid_risk.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:56.191000",
                        "description": "Violence Intensity over 3 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:56.191000",
                        "name": "conflictforecast_int_lnbest_03.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:55.855000",
                        "description": "Violence Intensity over 12 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:55.855000",
                        "name": "conflictforecast_int_lnbest_12.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:56.335000",
                        "description": "Any Violence over 3 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:56.335000",
                        "name": "conflictforecast_ons_anyviolence_03.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:55.719000",
                        "description": "Any Violence over 12 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:55.719000",
                        "name": "conflictforecast_ons_anyviolence_12.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:55.479000",
                        "description": "Armed Conflict over 3 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:55.479000",
                        "name": "conflictforecast_ons_armedconf_03.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:56.023000",
                        "description": "Armed Conflict over 12 months",
                        "format": "csv",
                        "last_modified": "2025-09-04T12:38:56.023000",
                        "name": "conflictforecast_ons_armedconf_12.csv",
                    },
                    {
                        "created": "2025-09-04T12:38:54.795000",
                        "description": "Codebook",
                        "format": "pdf",
                        "last_modified": "2025-09-04T12:38:54.795000",
                        "name": "codebook.pdf",
                    },
                ]
