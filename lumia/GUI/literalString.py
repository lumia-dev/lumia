import yaml
from yaml.representer import SafeRepresenter

class MyLiteralString(str):
    pass


def change_style(style, representer):
    def new_representer(dumper, data):
        scalar = representer(dumper, data)
        scalar.style = style
        return scalar
    return new_representer


represent_literal_str = change_style('|', SafeRepresenter.represent_str)


yaml.add_representer(MyLiteralString, represent_literal_str)

#markdown = """
## Markdown Document
#Foo Bar Baz
#"""

#data = {'string': MyLiteralString(markdown)}

#print(yaml.safe_dump(data))
