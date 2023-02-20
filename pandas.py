"""Pandas pseudo-modelform data handler."""
# Django
from django.core.exceptions import ValidationError
from django.db.models import NOT_PROVIDED
from django.utils.translation import gettext_lazy as _

# 3rd-party
import pandas as pd


class PandasForm():
    """Pandas abstract form with validation."""

    class Meta:
        fields = []

    def __init__(self, data=None):
        """Init pandas form."""
        self.opts = self.Meta
        self.fields = self.opts.fields
        self.cached_valid = True
        self.data = data
        self.schema = None
        self.pandas_data = None
        self.errors = {}
        self.row_data = dict()

    def handle_default(self, item):
        """Handle default value."""
        return None

    def _clean_data(self):
        """Clean form fields data."""
        self.cleaned_data = {}
        self.errors = self.schema.errors if self.schema else {}
        for idx, values in enumerate(self.data):
            self.cleaned_data[idx] = {}
            for item in self.fields:
                try:
                    self.cleaned_data[idx][item] = self.handle_default(item)
                    if item in values and values[item]:
                        self.cleaned_data[idx][item] = values[item]
                    func = getattr(self, f'clean_{item}', None)
                    self.row_data = self.cleaned_data[idx]
                    if func:
                        self.cleaned_data[idx][item] = func()
                except ValidationError as e:
                    self.cached_valid = False
                    if idx not in self.errors:
                        self.errors[idx] = {}
                    if item in self.errors[idx]:
                        self.errors[idx][item].append(e)
                    else:
                        self.errors[idx][item] = [e]
        self.row_data = dict()

    def add_error(self, field, error, row=None):
        """Add error like in django forms."""
        curr_field = '__all__'
        parsed_err = ValidationError(error)
        if field:
            curr_field = field
        if row and row not in self.errors:
            self.errors[row] = {}
        if row and curr_field in self.errors[row]:
            self.errors[row][curr_field].append(parsed_err)
        elif row:
            self.errors[row][curr_field] = [parsed_err]
        elif not row and '__all__' in self.errors:
            self.errors['__all__'].append(parsed_err)
        elif not row and '__all__' not in self.errors:
            self.errors['__all__'] = [parsed_err]

    def is_valid(self):
        """Check if dataset is valid."""
        if self.cached_valid is not None:
            return self.cached_valid
        self._clean_data()
        if self.schema:
            self.pandas_data = pd.DataFrame(
                self.cleaned_data.values(),
            )
            self.schema.validate(
                self.pandas_data,
            )
            return self.schema.valid and self.cached_valid
        return False


class FieldCheck(object):
    """Define field check."""

    def __init__(self, field_name, *args):
        self.field_name = field_name
        self.df = None


class IsInCheck(FieldCheck):
    """Define is in check with pandas data."""

    def __init__(self, *args):
        """Init check."""
        self.cached_list = args[1]
        super().__init__(*args)

    def validate(self, df, **kwargs):  # noqa: D102
        if kwargs.get('nullable'):
            return df[
                df[self.field_name].isin(self.cached_list) &
                df[self.field_name].isnull()
            ]
        return df[self.field_name].isin(self.cached_list)

    @staticmethod
    def get_error():
        return ValidationError(
            _('Komórka zawiera dane które nie zgadzają się z możliwymi wartościami!'),
        )


class LengthCheck(FieldCheck):
    """Define if it has certain length in pandas data."""

    def __init__(self, *args):  # noqa: D102
        self.length = 0
        if len(args) > 1:
            self.length = args[1]
        super().__init__(*args)

    def validate(self, df, **kwargs):  # noqa: D102
        if kwargs.get('nullable'):
            return df[
                df[self.field_name].str.len() <= self.length &
                df[self.field_name].isnull()
            ]
        return df[self.field_name].str.len() <= self.length

    def get_error(self):
        return ValidationError(
            _('Komórka przekracza maksymalną długość ({0} znaków)!').format(
                self.length,
            ),
        )


class PandasValidationColumn():
    """Create pandas validation frame."""

    def __init__(self, name, frame=None, checks=None, coerce=False, nullable=False):
        self.name = name
        self.df = frame
        self.checks = checks
        self.errors = {}
        self.coerce = coerce
        self.nullable = nullable

    def get_col_kwargs(self):
        return {
            'coerce': self.coerce,
            'nullable': self.nullable,
        }

    def handle_error(self, check, index):
        err_string = check.get_error()
        if index not in self.errors:
            self.errors[index] = {self.name: [err_string]}
        else:
            if self.name in self.errors[index]:
                self.errors[index][self.name].append(err_string)
            else:
                self.errors[index][self.name] = [err_string]

    def validate(self, pandas_data):
        for check in self.checks:
            validation = check.validate(pandas_data, **self.get_col_kwargs())
            for index, row in enumerate(validation):
                self.handle_error(check, index) if not row else None


class PandasValidationFrame(object):
    """Pandas validation frame."""

    def __init__(self, cached_fields=None):
        self.cached_fields = cached_fields
        self.errors = {}
        self.valid = True

    def bind_errors(self, errors):
        for idx, curr in errors.items():
            for key, val in curr.items():
                self.errors[idx][key] = val

    def validate(self, pandas_data):
        index = pandas_data.index
        # Init errors
        for val in range(index.start, index.stop):
            if val not in self.errors:
                self.errors[val] = {}
        for name, field in self.cached_fields.items():
            field.validate(pandas_data)
            if field.errors:
                self.valid = False
                self.bind_errors(field.errors)


class PandasModelForm(PandasForm):
    """Pandas abstract form extended for model data support."""

    class Meta:
        fields = []
        model = None

    def __init__(self, data=None):
        """Init pandas modelform."""
        super().__init__(data)
        self.model = getattr(self.opts, 'model', None)
        self.validation_fields = None
        self.cached_relations = dict()
        self.init_validation_types()
        self.schema = None
        self.cleaned_data = None
        self.errors = {}
        self.pandas_data = None
        if self.validation_fields:
            self.schema = PandasValidationFrame(self.validation_fields)

    def handle_relation(self, curr_field):
        """Handle model relation."""
        field_name = str(curr_field)
        if field_name not in self.cached_relations:
            rel_qs = curr_field.related_model.objects.values_list('id', flat=True)
            self.cached_relations[field_name] = [IsInCheck(
                curr_field.name, list(rel_qs)
            )]
        return self.cached_relations[field_name]

    def handle_type(self, curr_field):
        """Handle validation types."""
        type_name = type(curr_field).__name__
        checks = []
        if curr_field.max_length:
            checks.append(LengthCheck(curr_field.name, curr_field.max_length))
        if type_name in ['ForeignKey', 'ManyToManyField']:
            checks += self.handle_relation(curr_field)
        if curr_field.choices:
            valid_choices = list(dict(curr_field.choices).keys())
            checks.append(IsInCheck(curr_field.name, valid_choices))
        coerce = False
        if type_name in ['DecimalField', 'ForeignKey', 'ManyToManyField']:
            coerce = True
        return PandasValidationColumn(
            curr_field.name,
            self.pandas_data,
            checks=checks,
            coerce=coerce,
            nullable=bool(curr_field.null or curr_field.blank),
        )

    def init_validation_types(self):
        """Init validation cached dict."""
        cache_dict = {}
        for item in self.fields:
            curr_field = self.model._meta.get_field(item)
            cache_dict[item] = self.handle_type(curr_field)
        self.validation_fields = cache_dict

    def handle_default(self, item):
        """Handle default data type."""
        curr_field = self.model._meta.get_field(item)
        if curr_field.default != NOT_PROVIDED:
            return curr_field.default
