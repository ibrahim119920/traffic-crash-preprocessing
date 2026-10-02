"""Memory-bounded cleaning workflow for Chicago traffic crash data."""

from collections import Counter
from pathlib import Path
import re
import sqlite3

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


INJURY_MAP = {
    "injuries_fatal": "FATAL",
    "injuries_incapacitating": "INCAPACITATING INJURY",
    "injuries_non_incapacitating": "NONINCAPACITATING INJURY",
    "injuries_reported_not_evident": "REPORTED, NOT EVIDENT",
    "injuries_no_indication": "NO INDICATION OF INJURY",
}
INJURED_COLS = list(INJURY_MAP)[:4]
INJURY_COLS = list(INJURY_MAP) + ["injuries_unknown"]
NON_VEHICLE_UNITS = {"PEDESTRIAN", "BICYCLE", "NON-MOTOR VEHICLE"}
NON_DRIVER_PEOPLE = {"PASSENGER", "PEDESTRIAN", "BICYCLE", "NON-MOTOR VEHICLE", "NON-CONTACT VEHICLE"}
WKT_POINT = re.compile(r"^POINT\s*\(\s*([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s*\)$", re.I)

INTEGER_FIELDS = {
    "Crashes": {
        "posted_speed_limit", "lane_cnt", "street_no", "num_units", "crash_month",
        "crash_hour", "crash_day_of_week", "injuries_total", *INJURY_COLS,
    },
    "Vehicle": {"unit_no", "num_passengers", "occupant_cnt", "vehicle_year"},
    "People": {"seat_no", "age"},
}
FLOAT_FIELDS = {"Crashes": {"latitude", "longitude"}, "Vehicle": set(), "People": {"bac_result_value"}}
DATE_FIELDS = {"Crashes": {"crash_date", "date_police_notified"}, "Vehicle": {"crash_date"}, "People": {"crash_date"}}
PRIMARY_KEY = {"Crashes": "crash_record_id", "Vehicle": "crash_unit_id", "People": "person_id"}


class ParquetStream:
    def __init__(self, path):
        self.path = Path(path)
        self.writer = None
        self.rows = 0

    def write(self, frame):
        if frame.empty:
            return
        if self.writer is None:
            table = pa.Table.from_pandas(frame, preserve_index=False)
            self.writer = pq.ParquetWriter(self.path, table.schema, compression="zstd")
        else:
            table = pa.Table.from_pandas(frame, schema=self.writer.schema, preserve_index=False)
        self.writer.write_table(table)
        self.rows += len(frame)

    def close(self):
        if self.writer is not None:
            self.writer.close()


def parquet_chunks(path, chunksize):
    source = pq.ParquetFile(path)
    for batch in source.iter_batches(batch_size=chunksize):
        yield batch.to_pandas()


class MissingValuePipeline:
    def __init__(self, files, remaining_cols, initial_profile, data_dir, chunksize=20_000):
        self.files = files
        self.remaining_cols = remaining_cols
        self.initial_profile = initial_profile
        self.chunksize = chunksize
        self.outdir = Path(data_dir) / "processed"
        self.outdir.mkdir(exist_ok=True)
        self.workdir = self.outdir / "_work"
        self.workdir.mkdir(exist_ok=True)
        self.db_path = self.workdir / "relationships.sqlite"
        if self.db_path.exists():
            self.db_path.unlink()
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("PRAGMA cache_size=-32768")
        self.before = Counter()
        self.after = Counter()
        self.fills = Counter()
        self.metrics = Counter()
        self.rows = Counter()
        self.key_nulls = Counter()
        self.paths = {name: self.outdir / f"clean_{name.lower()}.parquet" for name in files}
        self.stage_paths = {name: self.workdir / f"stage_{name.lower()}.parquet" for name in files}

    def _fill(self, dataset, frame, column, mask, value, method):
        if column not in frame:
            return
        mask = (frame[column].isna() & mask).fillna(False)
        if isinstance(value, pd.Series):
            mask &= value.notna().fillna(False)
        elif pd.isna(value):
            return
        n = int(mask.sum())
        if n:
            frame.loc[mask, column] = value if not isinstance(value, pd.Series) else value.loc[mask]
            self.fills[dataset, column, method] += n

    def _source_chunks(self, dataset):
        return pd.read_csv(
            self.files[dataset], usecols=self.remaining_cols[dataset],
            dtype="string", keep_default_na=False, na_filter=False,
            chunksize=self.chunksize,
        )

    def _normalize(self, dataset, frame):
        for col in frame:
            frame[col] = frame[col].str.strip().mask(frame[col].str.strip().eq(""), pd.NA)
            self.before[dataset, col] += int(frame[col].isna().sum())
        if dataset == "People" and "age" in frame:
            frame["age_raw"] = frame["age"].copy()
        if dataset == "Vehicle" and "vehicle_year" in frame:
            frame["vehicle_year_raw"] = frame["vehicle_year"].copy()
        self.key_nulls[dataset] += int(frame[PRIMARY_KEY[dataset]].isna().sum())
        for col in INTEGER_FIELDS[dataset] & set(frame):
            old = frame[col].notna()
            value = pd.to_numeric(frame[col], errors="coerce").astype("Float64")
            value = value.mask(value.notna() & value.mod(1).ne(0), pd.NA)
            frame[col] = value.astype("Int64")
            self.metrics[dataset, f"invalid_numeric:{col}"] += int((old & frame[col].isna()).sum())
        for col in FLOAT_FIELDS[dataset] & set(frame):
            old = frame[col].notna()
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("Float64")
            self.metrics[dataset, f"invalid_numeric:{col}"] += int((old & frame[col].isna()).sum())
        for col in DATE_FIELDS[dataset] & set(frame):
            old = frame[col].notna()
            frame[col] = pd.to_datetime(frame[col], format="mixed", errors="coerce")
            self.metrics[dataset, f"invalid_date:{col}"] += int((old & frame[col].isna()).sum())
        if dataset == "People" and "age" in frame:
            invalid = frame["age"].notna() & ~frame["age"].between(0, 120)
            self.metrics[dataset, "invalid_domain:age"] += int(invalid.sum())
            frame.loc[invalid, "age"] = pd.NA
        if dataset == "Vehicle" and {"vehicle_year", "crash_date"} <= set(frame):
            year = frame["vehicle_year"]
            crash_year = frame["crash_date"].dt.year
            invalid = year.notna() & (year.lt(1900) | (crash_year.notna() & year.gt(crash_year + 1)))
            self.metrics[dataset, "invalid_domain:vehicle_year"] += int(invalid.sum())
            frame.loc[invalid, "vehicle_year"] = pd.NA
            if "unit_type" in frame:
                no_vehicle = frame["unit_type"].isin(NON_VEHICLE_UNITS) & frame["vehicle_year"].notna()
                self.metrics[dataset, "non_vehicle_year_removed"] += int(no_vehicle.sum())
                frame.loc[no_vehicle, "vehicle_year"] = pd.NA
        return frame

    def _same_table_crashes(self, frame):
        if {"latitude", "longitude", "location"} <= set(frame):
            lat, lon = frame["latitude"], frame["longitude"]
            bad_lat = lat.notna() & ~lat.between(-90, 90)
            bad_lon = lon.notna() & ~lon.between(-180, 180)
            self.metrics["Crashes", "invalid_latitude"] += int(bad_lat.sum())
            self.metrics["Crashes", "invalid_longitude"] += int(bad_lon.sum())
            frame.loc[bad_lat, "latitude"] = pd.NA
            frame.loc[bad_lon, "longitude"] = pd.NA
            extracted = frame["location"].str.extract(WKT_POINT)
            wlon = pd.to_numeric(extracted[0], errors="coerce")
            wlat = pd.to_numeric(extracted[1], errors="coerce")
            valid_wkt = wlat.between(-90, 90) & wlon.between(-180, 180)
            conflict = valid_wkt & (
                (frame["latitude"].notna() & (frame["latitude"] - wlat).abs().gt(0.0001))
                | (frame["longitude"].notna() & (frame["longitude"] - wlon).abs().gt(0.0001))
            )
            self.metrics["Crashes", "coordinate_wkt_conflict"] += int(conflict.sum())
            safe = valid_wkt & ~conflict
            self._fill("Crashes", frame, "latitude", safe, wlat.astype("Float64"), "WKT_LOCATION")
            self._fill("Crashes", frame, "longitude", safe, wlon.astype("Float64"), "WKT_LOCATION")
            good_pair = frame["latitude"].notna() & frame["longitude"].notna()
            point = pd.Series(pd.NA, index=frame.index, dtype="string")
            point.loc[good_pair] = [f"POINT ({x:.12g} {y:.12g})" for x, y in zip(frame.loc[good_pair, "longitude"], frame.loc[good_pair, "latitude"])]
            self._fill("Crashes", frame, "location", good_pair, point, "KOORDINAT_VALID")
        if {"injuries_total", *INJURED_COLS} <= set(frame):
            known = frame[INJURED_COLS].notna().all(axis=1)
            component_sum = frame[INJURED_COLS].sum(axis=1, min_count=4).astype("Int64")
            conflict = known & frame["injuries_total"].notna() & frame["injuries_total"].ne(component_sum)
            self.metrics["Crashes", "injury_total_component_conflict"] += int(conflict.sum())
            original = frame["injuries_total"].notna()
            self._fill("Crashes", frame, "injuries_total", known, component_sum, "JUMLAH_4_KOMPONEN")
            frame["injuries_total_source"] = pd.Series(np.where(original, "SUMBER", np.where(known, "JUMLAH_4_KOMPONEN", "NULL")), index=frame.index, dtype="string")
        return frame

    def stage_sources(self):
        for dataset in ("Vehicle", "People", "Crashes"):
            writer = ParquetStream(self.stage_paths[dataset])
            for chunk in self._source_chunks(dataset):
                chunk = self._normalize(dataset, chunk)
                if dataset == "Crashes":
                    chunk = self._same_table_crashes(chunk)
                writer.write(chunk)
                self.rows[dataset] += len(chunk)
            writer.close()
            assert self.rows[dataset] == int(self.initial_profile.loc[dataset, "baris"])

    def build_vehicle_index(self):
        db = self.db
        db.execute("CREATE TABLE vehicle_links (crash_record_id TEXT, crash_unit_id TEXT, vehicle_id TEXT, unit_type TEXT)")
        for frame in parquet_chunks(self.stage_paths["Vehicle"], self.chunksize):
            rows = ((None if pd.isna(a) else str(a), None if pd.isna(b) else str(b), None if pd.isna(c) else str(c), None if pd.isna(d) else str(d)) for a,b,c,d in zip(frame["crash_record_id"],frame["crash_unit_id"],frame["vehicle_id"],frame["unit_type"]))
            db.executemany("INSERT INTO vehicle_links VALUES (?,?,?,?)", rows)
        db.commit()
        db.execute("CREATE INDEX vehicle_pair_idx ON vehicle_links(crash_record_id,vehicle_id)")
        db.execute("CREATE INDEX vehicle_crash_idx ON vehicle_links(crash_record_id)")
        db.execute("CREATE TABLE vehicle_key AS SELECT crash_record_id,vehicle_id,COUNT(*) AS n,MIN(crash_unit_id) AS crash_unit_id FROM vehicle_links WHERE vehicle_id IS NOT NULL GROUP BY crash_record_id,vehicle_id")
        db.execute("CREATE UNIQUE INDEX vehicle_key_idx ON vehicle_key(crash_record_id,vehicle_id)")
        db.execute("CREATE TABLE unit_key AS SELECT crash_record_id,crash_unit_id,COUNT(*) AS n,MIN(unit_type) AS unit_type FROM vehicle_links GROUP BY crash_record_id,crash_unit_id")
        db.execute("CREATE UNIQUE INDEX unit_key_idx ON unit_key(crash_record_id,crash_unit_id)")
        db.execute("CREATE TABLE vehicle_by_crash AS SELECT crash_record_id,COUNT(*) AS unit_rows,CASE WHEN COUNT(*)=1 THEN MIN(crash_unit_id) END AS single_id,CASE WHEN COUNT(*)=1 THEN MIN(unit_type) END AS single_type FROM vehicle_links WHERE crash_record_id IS NOT NULL GROUP BY crash_record_id")
        db.execute("CREATE UNIQUE INDEX vehicle_by_crash_idx ON vehicle_by_crash(crash_record_id)")
        db.execute("CREATE TEMP TABLE tmp_people (row_idx INTEGER PRIMARY KEY,crash_record_id TEXT,vehicle_id TEXT,person_type TEXT,person_unit_candidate TEXT)")
        db.execute("CREATE TABLE people_summary (crash_record_id TEXT PRIMARY KEY,person_count INTEGER,n_fatal INTEGER,n_incapacitating INTEGER,n_non_incapacitating INTEGER,n_reported INTEGER,n_no_indication INTEGER,n_unclassified INTEGER)")
        db.execute("CREATE TABLE vehicle_people (crash_record_id TEXT,crash_unit_id TEXT,reported_people_count INTEGER,PRIMARY KEY(crash_record_id,crash_unit_id))")
        db.commit()

    @staticmethod
    def _person_unit_compatible(person_type, unit_type):
        if pd.isna(person_type) or pd.isna(unit_type):
            return False
        if person_type == "PASSENGER":
            return unit_type == "DRIVER"
        return person_type == unit_type and person_type in {"DRIVER", "PEDESTRIAN", "BICYCLE", "NON-MOTOR VEHICLE", "NON-CONTACT VEHICLE"}

    def _link_people(self, frame):
        db = self.db
        db.execute("DELETE FROM tmp_people")
        candidates = frame["person_id"].str.extract(r"^O(\d+)$", expand=False)
        db.executemany("INSERT INTO tmp_people VALUES (?,?,?,?,?)", (
            (i, None if pd.isna(crash) else str(crash), None if pd.isna(vehicle) else str(vehicle), None if pd.isna(kind) else str(kind), None if pd.isna(candidate) else str(candidate))
            for i,(crash,vehicle,kind,candidate) in enumerate(zip(frame["crash_record_id"],frame["vehicle_id"],frame["person_type"],candidates))
        ))
        rows = db.execute("SELECT p.row_idx,k.n,k.crash_unit_id,v.unit_rows,v.single_id,v.single_type,s.n,s.crash_unit_id,s.unit_type FROM tmp_people p LEFT JOIN vehicle_key k ON p.crash_record_id=k.crash_record_id AND p.vehicle_id=k.vehicle_id LEFT JOIN vehicle_by_crash v ON p.crash_record_id=v.crash_record_id LEFT JOIN unit_key s ON p.crash_record_id=s.crash_record_id AND p.person_unit_candidate=s.crash_unit_id ORDER BY p.row_idx").fetchall()
        raw_id = frame["vehicle_id"].copy()
        frame["vehicle_id_raw"] = raw_id
        frame["vehicle_id_was_missing"] = raw_id.isna().astype("boolean")
        link = pd.Series(pd.NA, index=frame.index, dtype="string")
        status = pd.Series(pd.NA, index=frame.index, dtype="string")
        for row_idx, matched_n, matched_unit, unit_rows, single_id, single_type, suffix_n, suffix_unit, suffix_type in rows:
            idx = frame.index[row_idx]
            source_id = raw_id.iloc[row_idx]
            person_type = frame["person_type"].iloc[row_idx]
            if pd.notna(source_id):
                if matched_n == 1:
                    link.at[idx] = matched_unit
                    status.at[idx] = "COCOK"
                elif matched_n is not None:
                    status.at[idx] = "AMBIGU"
                else:
                    status.at[idx] = "TIDAK_COCOK"
            elif pd.notna(person_type) and suffix_n == 1 and person_type == suffix_type:
                link.at[idx] = suffix_unit
                status.at[idx] = "UNIT_DARI_PERSON_ID"
            elif unit_rows == 1 and single_id is not None and self._person_unit_compatible(person_type, single_type):
                link.at[idx] = single_id
                status.at[idx] = "REKONSTRUKSI_UNIK"
            elif unit_rows is None:
                status.at[idx] = "TIDAK_ADA_UNIT"
            elif unit_rows > 1:
                status.at[idx] = "AMBIGU"
            else:
                status.at[idx] = "TIDAK_DAPAT_DIPASTIKAN"
        frame["linked_crash_unit_id"] = link
        frame["vehicle_link_status"] = status
        self.metrics["People", "linked_crash_unit_id_success"] += int(link.notna().sum())
        self.metrics["People", "linked_crash_unit_id_failure"] += int(link.isna().sum())
        counts = status.value_counts(dropna=False)
        for label,n in counts.items():
            self.metrics["People", f"vehicle_join:{label}"] += int(n)
        return frame

    def _categorize_people(self, frame):
        frame["age_was_missing"] = frame["age"].isna().astype("boolean") if "age" in frame else pd.Series(True,index=frame.index,dtype="boolean")
        if "age" in frame:
            ages = frame["age"].to_numpy(dtype="float64", na_value=np.nan)
            frame["age_group"] = pd.Series(pd.cut(ages,bins=[-1,17,24,44,64,float("inf")],labels=["0-17","18-24","25-44","45-64","65+"]),index=frame.index).astype("string")
            self.before["People", "age_group"] += int(frame["age_group"].isna().sum())
            self._fill("People", frame, "age_group", pd.Series(True,index=frame.index), "TIDAK_TERCATAT", "STATUS_TIDAK_TERCATAT")
            self.after["People", "age_group"] += int(frame["age_group"].isna().sum())
        for col in ("city", "state", "sex", "bac_result", "injury_classification"):
            self._fill("People", frame, col, pd.Series(True,index=frame.index), "TIDAK_TERCATAT", "STATUS_TIDAK_TERCATAT")
        person_type = frame["person_type"]
        non_driver = person_type.isin(NON_DRIVER_PEOPLE)
        for col in ("driver_vision", "driver_action", "physical_condition"):
            self._fill("People", frame, col, non_driver, "TIDAK_BERLAKU", "STATUS_TIDAK_BERLAKU")
            self._fill("People", frame, col, pd.Series(True,index=frame.index), "TIDAK_TERCATAT", "STATUS_TIDAK_TERCATAT")
        no_airbag = person_type.isin({"PEDESTRIAN", "BICYCLE", "NON-MOTOR VEHICLE"})
        self._fill("People", frame, "airbag_deployed", no_airbag, "TIDAK_BERLAKU", "STATUS_TIDAK_BERLAKU")
        self._fill("People", frame, "airbag_deployed", pd.Series(True,index=frame.index), "TIDAK_TERCATAT", "STATUS_TIDAK_TERCATAT")
        self._fill("People", frame, "ejection", person_type.eq("PEDESTRIAN"), "TIDAK_BERLAKU", "STATUS_TIDAK_BERLAKU")
        for col in ("ejection", "safety_equipment"):
            self._fill("People", frame, col, pd.Series(True,index=frame.index), "TIDAK_TERCATAT", "STATUS_TIDAK_TERCATAT")
        return frame

    def _accumulate_people(self, frame):
        db = self.db
        valid_labels = set(INJURY_MAP.values())
        summary = pd.DataFrame({"crash_record_id":frame["crash_record_id"],"person_count":1})
        injury = frame["injury_classification"]
        names = [("n_fatal","FATAL"),("n_incapacitating","INCAPACITATING INJURY"),("n_non_incapacitating","NONINCAPACITATING INJURY"),("n_reported","REPORTED, NOT EVIDENT"),("n_no_indication","NO INDICATION OF INJURY")]
        for name,label in names:
            summary[name] = injury.eq(label).fillna(False).astype("int64")
        summary["n_unclassified"] = (~injury.isin(valid_labels)).fillna(True).astype("int64")
        grouped = summary.groupby("crash_record_id",dropna=True,sort=False).sum(numeric_only=True).reset_index()
        cols = ["person_count", *[x[0] for x in names], "n_unclassified"]
        updates = ",".join(f"{col}=people_summary.{col}+excluded.{col}" for col in cols)
        db.executemany(f"INSERT INTO people_summary VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(crash_record_id) DO UPDATE SET {updates}", grouped[["crash_record_id",*cols]].itertuples(index=False,name=None))
        linked = frame.loc[frame["linked_crash_unit_id"].notna() & frame["person_type"].isin({"DRIVER","PASSENGER"}),["crash_record_id","linked_crash_unit_id"]]
        if not linked.empty:
            linked = linked.groupby(["crash_record_id","linked_crash_unit_id"],dropna=True).size().reset_index(name="reported_people_count")
            db.executemany("INSERT INTO vehicle_people VALUES (?,?,?) ON CONFLICT(crash_record_id,crash_unit_id) DO UPDATE SET reported_people_count=vehicle_people.reported_people_count+excluded.reported_people_count",linked.itertuples(index=False,name=None))
        db.commit()

    def clean_people(self):
        writer = ParquetStream(self.paths["People"])
        for frame in parquet_chunks(self.stage_paths["People"],self.chunksize):
            frame = self._link_people(frame)
            frame = self._categorize_people(frame)
            self._accumulate_people(frame)
            self._count_after("People",frame)
            writer.write(frame)
        writer.close()
        self.db.execute("CREATE TEMP TABLE tmp_vehicles (row_idx INTEGER PRIMARY KEY,crash_record_id TEXT,crash_unit_id TEXT)")
        self.db.execute("CREATE TEMP TABLE tmp_crashes (row_idx INTEGER PRIMARY KEY,crash_record_id TEXT)")

    def _vehicle_people_count(self, frame):
        db=self.db
        db.execute("DELETE FROM tmp_vehicles")
        db.executemany("INSERT INTO tmp_vehicles VALUES (?,?,?)",((i,None if pd.isna(c) else str(c),None if pd.isna(u) else str(u)) for i,(c,u) in enumerate(zip(frame["crash_record_id"],frame["crash_unit_id"]))))
        rows=db.execute("SELECT t.row_idx,v.reported_people_count FROM tmp_vehicles t LEFT JOIN vehicle_people v ON t.crash_record_id=v.crash_record_id AND t.crash_unit_id=v.crash_unit_id ORDER BY t.row_idx").fetchall()
        return pd.Series(pd.array([row[1] for row in rows],dtype="Int64"),index=frame.index)

    def clean_vehicles(self):
        writer=ParquetStream(self.paths["Vehicle"])
        for frame in parquet_chunks(self.stage_paths["Vehicle"],self.chunksize):
            frame["reported_people_count"] = self._vehicle_people_count(frame)
            non_vehicle=frame["unit_type"].isin(NON_VEHICLE_UNITS)
            self._fill("Vehicle",frame,"unit_type",pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            if "vehicle_year" in frame:
                frame["vehicle_year_was_missing"]=frame["vehicle_year"].isna().astype("boolean")
                frame["vehicle_year_status"]=pd.Series(np.where(frame["vehicle_year"].notna(),"TERCATAT",np.where(non_vehicle,"TIDAK_BERLAKU","TIDAK_TERCATAT")),index=frame.index,dtype="string")
            for col in ("lic_plate_state","make","model","vehicle_use","vehicle_type","vehicle_defect"):
                self._fill("Vehicle",frame,col,non_vehicle,"TIDAK_BERLAKU","STATUS_TIDAK_BERLAKU")
                self._fill("Vehicle",frame,col,pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            for col in ("first_contact_point","maneuver","travel_direction"):
                self._fill("Vehicle",frame,col,pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            if "occupant_cnt" in frame:
                comparable=frame["occupant_cnt"].notna() & frame["reported_people_count"].notna()
                self.metrics["Vehicle","occupant_count_comparable"] += int(comparable.sum())
                self.metrics["Vehicle","occupant_count_conflict"] += int((comparable & frame["occupant_cnt"].ne(frame["reported_people_count"])).sum())
            self._count_after("Vehicle",frame)
            writer.write(frame)
        writer.close()

    def _crash_summaries(self,frame):
        db=self.db
        db.execute("DELETE FROM tmp_crashes")
        db.executemany("INSERT INTO tmp_crashes VALUES (?,?)",((i,None if pd.isna(c) else str(c)) for i,c in enumerate(frame["crash_record_id"])))
        rows=db.execute("SELECT t.row_idx,v.unit_rows,p.person_count,p.n_fatal,p.n_incapacitating,p.n_non_incapacitating,p.n_reported,p.n_no_indication,p.n_unclassified FROM tmp_crashes t LEFT JOIN vehicle_by_crash v ON t.crash_record_id=v.crash_record_id LEFT JOIN people_summary p ON t.crash_record_id=p.crash_record_id ORDER BY t.row_idx").fetchall()
        cols=["row_idx","vehicle_unit_count","people_count","n_fatal","n_incapacitating","n_non_incapacitating","n_reported","n_no_indication","n_unclassified"]
        result=pd.DataFrame(rows,columns=cols).set_index("row_idx")
        result.index=frame.index
        for col in cols[1:]:
            result[col]=result[col].astype("Int64")
        return result

    def clean_crashes(self):
        writer=ParquetStream(self.paths["Crashes"])
        people_names={"injuries_fatal":"n_fatal","injuries_incapacitating":"n_incapacitating","injuries_non_incapacitating":"n_non_incapacitating","injuries_reported_not_evident":"n_reported","injuries_no_indication":"n_no_indication"}
        for frame in parquet_chunks(self.stage_paths["Crashes"],self.chunksize):
            summary=self._crash_summaries(frame)
            frame["vehicle_unit_count"]=summary["vehicle_unit_count"]
            frame["people_count"]=summary["people_count"]
            self.metrics["Crashes","join_vehicles_success"] += int(frame["vehicle_unit_count"].notna().sum())
            self.metrics["Crashes","join_people_success"] += int(frame["people_count"].notna().sum())
            if "num_units" in frame:
                comparable=frame["num_units"].notna() & frame["vehicle_unit_count"].notna()
                self.metrics["Crashes","num_units_conflict"] += int((comparable & frame["num_units"].ne(frame["vehicle_unit_count"])).sum())
            existing=frame["injuries_total"].notna() if "injuries_total" in frame else pd.Series(False,index=frame.index)
            complete=summary["people_count"].notna() & summary["n_unclassified"].eq(0)
            people_injured=summary[["n_fatal","n_incapacitating","n_non_incapacitating","n_reported"]].sum(axis=1,min_count=4).astype("Int64")
            consistent=complete & existing & frame["injuries_total"].eq(people_injured)
            conflict=complete & existing & frame["injuries_total"].ne(people_injured)
            for crash_col,person_col in people_names.items():
                if crash_col in frame:
                    known=frame[crash_col].notna()
                    diff=known & complete & frame[crash_col].ne(summary[person_col])
                    conflict |= diff
                    consistent &= ~diff
            if "injuries_unknown" in frame:
                diff=frame["injuries_unknown"].notna() & complete & frame["injuries_unknown"].ne(0)
                conflict |= diff
                consistent &= ~diff
            self.metrics["Crashes","people_injury_conflict"] += int(conflict.sum())
            frame["people_injury_conflict"] = conflict.fillna(False).astype("boolean")
            frame["people_injury_reconstruction_valid"] = consistent.fillna(False).astype("boolean")
            for crash_col,person_col in people_names.items():
                if crash_col in INJURED_COLS and crash_col in frame:
                    self._fill("Crashes",frame,crash_col,consistent,summary[person_col],"PEOPLE_LENGKAP_KONSISTEN")
            if {"injuries_total",*INJURED_COLS} <= set(frame):
                known=frame[INJURED_COLS].notna().all(axis=1)
                sums=frame[INJURED_COLS].sum(axis=1,min_count=4).astype("Int64")
                self._fill("Crashes",frame,"injuries_total",known,sums,"JUMLAH_4_KOMPONEN_SETELAH_JOIN")
                frame.loc[frame["injuries_total_source"].eq("NULL") & known,"injuries_total_source"]="JUMLAH_4_KOMPONEN_SETELAH_JOIN"
                total_match=frame["injuries_total"].notna() & known & frame["injuries_total"].eq(sums)
                self.metrics["Crashes","injury_total_consistent"] += int(total_match.sum())
                self.metrics["Crashes","injury_total_inconsistent"] += int((frame["injuries_total"].notna() & known & frame["injuries_total"].ne(sums)).sum())
            if "most_severe_injury" in frame:
                self._fill("Crashes",frame,"most_severe_injury",pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            self._fill("Crashes",frame,"report_type",pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            if {"street_direction","street_name"} <= set(frame):
                direction=frame["street_name"].str.extract(r"^([NSEW])\s+",expand=False).astype("string")
                self._fill("Crashes",frame,"street_direction",direction.notna(),direction,"ARAH_EKSPLISIT_NAMA_JALAN")
                self._fill("Crashes",frame,"street_direction",pd.Series(True,index=frame.index),"TIDAK_TERCATAT","STATUS_TIDAK_TERCATAT")
            if {"latitude","longitude"} <= set(frame):
                frame["geo_missing"]=(frame["latitude"].isna() | frame["longitude"].isna()).astype("boolean")
                self.metrics["Crashes","geo_missing"] += int(frame["geo_missing"].sum())
                valid=frame["latitude"].between(-90,90) & frame["longitude"].between(-180,180)
                self.metrics["Crashes","valid_coordinates"] += int(valid.fillna(False).sum())
            self._count_after("Crashes",frame)
            writer.write(frame)
        writer.close()

    def _count_after(self,dataset,frame):
        for col in self.remaining_cols[dataset]:
            self.after[dataset,col] += int(frame[col].isna().sum())

    def finish(self):
        self.metrics["Crashes", "join_people_failure"] = self.rows["Crashes"] - self.metrics["Crashes", "join_people_success"]
        self.metrics["Crashes", "join_vehicles_failure"] = self.rows["Crashes"] - self.metrics["Crashes", "join_vehicles_success"]
        for status in ("COCOK", "UNIT_DARI_PERSON_ID", "REKONSTRUKSI_UNIK", "TIDAK_COCOK", "AMBIGU", "TIDAK_ADA_UNIT", "TIDAK_DAPAT_DIPASTIKAN"):
            self.metrics["People", f"vehicle_join:{status}"] += 0
        self.db.commit()
        self.db.close()
        audit=[]
        for dataset,columns in self.remaining_cols.items():
            filled_extra = sorted({col for ds,col,_ in self.fills if ds == dataset} - set(columns))
            for col in [*columns, *filled_extra]:
                methods={method:n for (ds,c,method),n in self.fills.items() if ds==dataset and c==col}
                if not methods:
                    methods={"TIDAK_DIISI":0}
                for method,n in methods.items():
                    audit.append({"dataset":dataset,"kolom":col,"kosong_sebelum":self.before[dataset,col],"kosong_sesudah":self.after[dataset,col],"metode":method,"jumlah_diisi":n,"tetap_null":self.after[dataset,col],"metrik":"","jumlah_metrik":pd.NA})
        for (dataset,metric),n in sorted(self.metrics.items()):
            audit.append({"dataset":dataset,"kolom":"","kosong_sebelum":pd.NA,"kosong_sesudah":pd.NA,"metode":"AUDIT","jumlah_diisi":0,"tetap_null":pd.NA,"metrik":metric,"jumlah_metrik":n})
        audit=pd.DataFrame(audit)
        audit_path=self.outdir / "missing_value_audit.csv"
        audit.to_csv(audit_path,index=False)
        validation=[]
        for dataset,path in self.paths.items():
            parquet=pq.ParquetFile(path)
            schema=parquet.schema_arrow
            key=PRIMARY_KEY[dataset]
            key_hashes=[]
            for batch in parquet.iter_batches(batch_size=self.chunksize,columns=[key]):
                keys=pd.Series(batch.column(0).to_pandas(),dtype="string")
                key_hashes.append(pd.util.hash_pandas_object(keys,index=False).to_numpy())
            hashes=np.concatenate(key_hashes)
            duplicate_keys=int(len(hashes)-np.unique(hashes).size)
            numeric_ok=all(pa.types.is_integer(schema.field(col).type) for col in INTEGER_FIELDS[dataset] & set(schema.names))
            numeric_ok &= all(pa.types.is_floating(schema.field(col).type) for col in FLOAT_FIELDS[dataset] & set(schema.names))
            validation.append({
                "dataset":dataset,"baris_sumber":int(self.initial_profile.loc[dataset,"baris"]),
                "baris_bersih":parquet.metadata.num_rows,"duplikat_id_sumber":int(self.initial_profile.loc[dataset,"duplikat_id"]),
                "duplikat_id_bersih":duplicate_keys,"kunci_kosong":self.key_nulls[dataset],"kolom_bersih":len(schema),
                "tipe_numerik_valid":numeric_ok,
                "tipe_koordinat":str(schema.field("latitude").type) if dataset=="Crashes" and "latitude" in schema.names else "-",
                "tipe_umur":str(schema.field("age").type) if dataset=="People" and "age" in schema.names else "-",
                "tipe_tahun_kendaraan":str(schema.field("vehicle_year").type) if dataset=="Vehicle" and "vehicle_year" in schema.names else "-",
            })
        validation=pd.DataFrame(validation)
        assert (validation["baris_sumber"]==validation["baris_bersih"]).all(),"Jumlah baris berubah"
        assert (validation["duplikat_id_sumber"]==0).all() and (validation["duplikat_id_bersih"]==0).all() and (validation["kunci_kosong"]==0).all(),"Kunci sumber atau hasil bermasalah"
        assert validation["tipe_numerik_valid"].all(),"Tipe numerik hasil tidak sesuai"
        for path in [*self.stage_paths.values(),self.db_path]:
            if path.exists():
                path.unlink()
        self.workdir.rmdir()
        return audit,validation

    def run(self):
        self.stage_sources()
        self.build_vehicle_index()
        self.clean_people()
        self.clean_vehicles()
        self.clean_crashes()
        return self.finish()
