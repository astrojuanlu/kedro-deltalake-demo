import typing as t
import datetime as dt

import polars as pl
from kedro.io import AbstractDataset


class PolarsDeltaDataset(AbstractDataset[pl.DataFrame, pl.DataFrame]):
    def __init__(
        self,
        filepath: str,
        delta_version: int | str | dt.datetime | None = None,
        load_args: dict[str, t.Any] | None = None,
        save_args: dict[str, t.Any] | None = None,
        credentials: dict[str, t.Any] | None = None,
        storage_options: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
    ):
        self._filepath = filepath
        self._delta_version = delta_version  # Cannot be called _version
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self._credentials = credentials or {}
        self._storage_options = storage_options or {}

        self._storage_options.update(self._credentials)
        self._metadata = metadata or {}
        self._merge_predicate = self._save_args.pop("merge_predicate", None)

    def _load(self) -> pl.DataFrame:
        return pl.read_delta(
            self._filepath, version=self._delta_version, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pl.DataFrame) -> None:
        if self._save_args.get("mode") == "merge":
            if not self._merge_predicate:
                raise ValueError("Missing `merge_predicate` for merge mode")

            (
                data.write_delta(
                    self._filepath,
                    storage_options=self._storage_options,
                    delta_merge_options={
                        "predicate": self._merge_predicate,
                        "source_alias": "s",
                        "target_alias": "t",
                    },
                    **self._save_args,
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        else:
            data.write_delta(
                self._filepath, storage_options=self._storage_options, **self._save_args
            )

    def _describe(self) -> dict[str, t.Any]:
        return dict(
            filepath=self._filepath,
            load_args=self._load_args,
            save_args=self._save_args,
            storage_options=self._storage_options,
            metadata=self._metadata,
        )
