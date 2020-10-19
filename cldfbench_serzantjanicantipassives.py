from collections import ChainMap, defaultdict, OrderedDict
import pathlib
import re

from pybtex.database import parse_string
import cldfcatalog
from cldfbench import Dataset as BaseDataset
from cldfbench.cldf import CLDFSpec
from cldfbench.catalogs import Glottolog


PARAMETER_COLUMNS = (
    ('AP marker', 'ap-marker'),
    ('Type of AP Marker', 'marker-type'),
    ('FunctionAP', 'functions'),
    ('Polysemy', 'polysemy'),
    ('Productivity of AP', 'productivity'),
    ('Obligatoriness of P', 'p-obligatoriness'),
    ('Definiteness P', 'p-definiteness'),
)


NA_SYNONYMS = {'NI', '_inapplicable', 'NA', 'n/a'}


def normalise_row(row):
    return {
        k.strip(): v.strip()
        for k, v in row.items()
        if k.strip() and v.strip()}


def normalise_table(table):
    return list(filter(None, map(normalise_row, table)))


def title_case(s):
    return re.sub(r'\w+', lambda m: m.group(0).capitalize(), s)


def make_language_table(lang_info):
    glottocodes = sorted({l['ID'] for l in lang_info.values()})

    catconf = cldfcatalog.Config.from_file()
    glottolog_path = catconf.get_clone('glottolog')
    glottolog = Glottolog(glottolog_path).api
    nodemap = {l.id: l for l in glottolog.languoids(ids=glottocodes)}

    languages = [
        ChainMap(
            {
                'ID': glottocode,
                'Glottocode': glottocode,
                'ISO639P3code': nodemap[glottocode].iso,
            },
            {k: v for k, v in lang_info[glottocode].items() if k and v},
            {
                'Name': nodemap[glottocode].name,
                'Macroarea': nodemap[glottocode].macroareas[0].name if nodemap[glottocode].macroareas else '',
                'Latitude': nodemap[glottocode].latitude,
                'Longitude': nodemap[glottocode].longitude,
            })
        for glottocode in glottocodes]

    return languages


def unify_na(value):
    return 'n/a' if value in NA_SYNONYMS else value


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "serzantjanicantipassives"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(
            dir=self.cldf_dir,
            module='StructureDataset',
            metadata_fname='cldf-metadata.json')

    def cmd_download(self, args):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw_dir`, e.g.

        >>> self.raw_dir.download(url, fname)
        """
        pass

    def cmd_makecldf(self, args):
        """
        Convert the raw data to a CLDF dataset.

        >>> args.writer.objects['LanguageTable'].append(...)
        """

        # CLDF schema

        args.writer.cldf.add_component('ParameterTable')
        args.writer.cldf.add_component('LanguageTable', 'SubBranch', 'Family')
        args.writer.cldf.add_component('CodeTable')

        args.writer.cldf.add_table(
            'constructions.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'http://cldf.clld.org/v1.0/terms.rdf#name',
            'http://cldf.clld.org/v1.0/terms.rdf#description',
            'http://cldf.clld.org/v1.0/terms.rdf#languageReference',
            'http://cldf.clld.org/v1.0/terms.rdf#source')
        args.writer.cldf.add_table(
            'cvalues.csv',
            'http://cldf.clld.org/v1.0/terms.rdf#id',
            'Construction_ID',
            'http://cldf.clld.org/v1.0/terms.rdf#parameterReference',
            'http://cldf.clld.org/v1.0/terms.rdf#value',
            'http://cldf.clld.org/v1.0/terms.rdf#codeReference',
            'http://cldf.clld.org/v1.0/terms.rdf#comment')

        args.writer.cldf.add_foreign_key(
            'cvalues.csv', 'Construction_ID', 'constructions.csv', 'ID')

        # Read data

        data = self.raw_dir.read_csv('Data_to_be_published.csv', dicts=True)
        data = normalise_table(data)
        parameters = self.etc_dir.read_csv('parameters.csv', dicts=True)
        source_map = {
            citation.strip(): key.strip()
            for key, citation in self.etc_dir.read_csv('citations-to-bibtex.csv')}
        sources = parse_string(self.raw_dir.read('sources.bib'), 'bibtex')

        # Process data

        lang_info = {
            row['Glottolog.Name']:
            {
                'ID': row['Glottolog.Name'],
                'Name': title_case(row.get('Language', '')),
                'SubBranch': title_case(row.get('Sub-branch', '')),
                'Family': title_case(row.get('Family', '')),
            }
            for row in data}
        languages = OrderedDict(
            (l['ID'], l)
            for l in make_language_table(lang_info))

        code_dict = OrderedDict()
        for column, param_id in PARAMETER_COLUMNS:
            if param_id == 'ap-marker':
                continue
            code_dict[param_id] = sorted({
                unify_na(row[column])
                for row in data
                if row.get(column)})
        codes = OrderedDict(
            (
                (param_id, name),
                {
                    'ID': '{}-c{}'.format(param_id, index + 1),
                    'Parameter_ID': param_id,
                    'Name': name,
                },
            )
            for param_id, code_names in code_dict.items()
            for index, name in enumerate(code_names))

        constructions = []
        cvalues = []
        ords = defaultdict(int)
        for index, row in enumerate(data):
            lang_id = row['Glottolog.Name']
            lang_name = languages[row['Glottolog.Name']]['Name']
            ords[lang_id] += 1

            constr_ord = ords[lang_id]
            constr_id = '{}-ap{}'.format(lang_id, constr_ord)

            def known_citation(cite):
                if cite in source_map:
                    return True
                else:
                    print(
                        'row {}: unknown citation:'.format(index + 2),
                        cite,
                        file=sys.stderr)
                    return False
            citations = [
                source_map[citation.strip()]
                for citation in row.get('Source', '').splitlines()
                if known_citation(citation)]

            constructions.append({
                'ID': constr_id,
                'Name': '{} Antipassive Construction {}'.format(lang_name, constr_ord),
                'Language_ID': lang_id,
                'Source': citations})

            cvalues.extend(
                {
                    'ID': '{}-{}'.format(constr_id, param_id),
                    'Construction_ID': constr_id,
                    'Parameter_ID': param_id,
                    'Value': unify_na(row[column]),
                    'Code_ID': codes.get((param_id, unify_na(row[column])), {}).get('ID'),
                }
                for column, param_id in PARAMETER_COLUMNS
                if row.get(column))

        # Output data

        args.writer.cldf.add_sources(sources)
        args.writer.objects['LanguageTable'] = languages.values()
        args.writer.objects['ParameterTable'] = parameters
        args.writer.objects['CodeTable'] = codes.values()
        args.writer.objects['ValueTable'] = []
        args.writer.objects['constructions.csv'] = constructions
        args.writer.objects['cvalues.csv'] = cvalues
