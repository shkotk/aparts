import re


class ParamRule:
    def __init__(self, key: str, value_map: dict) -> None:
        self._key = key
        self._value_map = value_map

    def extract(self, olxAd: dict) -> str:
        highlights = ''
        for param in olxAd['params']:
            if param['key'] == self._key:
                value = param['normalizedValue']
                if type(value) is list:
                    highlights += ''.join(map(lambda v: self._value_map.get(v, ''), value))
                else:
                    highlights += self._value_map.get(value, '')

        return highlights


class PropRule:
    def __init__(self, path: str, match: str, value_map: dict) -> None:
        self._path = path.split('.')
        self._value_map = value_map
        if match == 'contains':
            self._match = lambda expected, actual: re.search(expected, actual, re.IGNORECASE)
        elif match == 'exact':
            self._match = lambda expected, actual: actual == expected
        else:
            raise Exception(f'unexpected property rule match type "{self.match}"')

    def extract(self, olxAd: dict) -> str:
        highlights = ''
        prop_value = self._get_prop_value(olxAd)
        for value, highlight in self._value_map.items():
            if self._match(value, prop_value):
                highlights += highlight

        return highlights

    def _get_prop_value(self, olxAd: dict) -> str:
        node = olxAd
        for path_part in self._path:
            node = node[path_part]
        return node


def parse_rules(config: list) -> list:
    if len(config) == 0:
        return []

    rules = []
    for rule_config in config:
        rule_type = rule_config['type']
        if rule_type == 'param':
            rules.append(ParamRule(rule_config['key'], rule_config['valueMap']))
        elif rule_type == 'prop':
            rules.append(PropRule(rule_config['path'], rule_config['match'], rule_config['valueMap']))
        else:
            raise Exception(f'unexpected rule type "{rule_type}"')

    return rules
