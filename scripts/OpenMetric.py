#!/usr/bin/env python3

from collections import defaultdict

class MetricFamily:
    def __init__(self, name: str, mtype: str | None = None, munit: str | None = None, mhelp: str | None = None):
        self.name = name
        self.mtype = mtype
        self.munit = munit
        self.mhelp = mhelp

    def __str__(self):
        res = []
        res.append(f"# TYPE {self.name} {self.mtype}\n")
        if self.munit:
            res.append(f"# UNIT {self.name} {self.munit}\n")
        if self.mhelp:
            res.append(f"# HELP {self.name} {self.mhelp}\n")
        return "".join(res)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

class Metric:
    def __init__(self,
                 name: str,
                 value,
                 labels: dict[str,str] | None = None,
                 mtype: str | None = None,
                 mhelp: str | None = None,
                 munit: str | None = None,
                 timestamp: int | None = None):
        self.name = f"{name}_{munit}" if munit else name
        self.labels = dict(sorted(labels.items(), key=lambda item:item[1])) if labels else None
        self.mtype = mtype
        self.munit = munit
        self.mhelp = mhelp
        self.value = value
        self.timestamp = timestamp

    def get_family_name(self) -> str:
        if self.munit is None:
            return self.name
        index_unit = self.name.find(f"_{self.munit}")
        return self.name[0:index_unit+len(self.munit)+1] if index_unit != -1 else self.name

    def get_family(self) -> MetricFamily:
        return MetricFamily(self.get_family_name(), mtype = self.mtype, munit = self.munit, mhelp = self.mhelp)

    def __str__(self):
        res = []
        res.append(self.name)

        if self.labels:
            res.append("{")
            for label, value in self.labels.items():
                res.append(f'{label}="{value}"')
                res.append(",")
            res.pop()
            res.append("}")

        res.append(" ")
        res.append(str(self.value))
        if self.timestamp:
            res.append(" ")
            res.append(str(self.timestamp))

        return "".join(res)

    def __lt__(self, other):
        if not isinstance(other, Metric):
            raise TypeError("Can only compare Metric with Metric")

        # Avoid interleaving Metrics or MetricPoints
        # Family name should be identical
        if self.get_family_name() != other.get_family_name():
            return self.get_family_name() < other.get_family_name()

        if self.labels is None and other.labels is not None:
            return True
        if self.labels is not None and other.labels is None:
            return False
        if self.labels is not None and other.labels is not None:
            # Sort by labels alphabetically
            self_labels_sorted = sorted(self.labels.items())
            other_labels_sorted = sorted(other.labels.items())
            for (self_key, self_value), (other_key, other_value) in zip(self_labels_sorted, other_labels_sorted):
                if self_key != other_key:
                    return self_key < other_key
                if self_value != other_value:
                    return self_value < other_value

        # Compare timestamps
        if self.timestamp is None and other.timestamp is not None:
            return True
        if self.timestamp is not None and other.timestamp is None:
            return False
        if self.timestamp is not None and other.timestamp is not None:
            return self.timestamp < other.timestamp

        # Finally, compare values if everything else is equal
        return self.value < other.value


class MetricSet:
    def __init__(self, families: defaultdict[MetricFamily, list[Metric]] = defaultdict(list)):
        self.families = families

    def insert(self, metric: Metric):
        self.families[metric.get_family()].append(metric)

    def __str__(self):
        res = []
        for mfam, ms in self.families.items():
            res.append(str(mfam))
            ms.sort()
            for m in ms:
                res.append(f"{m}\n")
        if res:
            res.append("# EOF")
        return "".join(dict.fromkeys(res))
