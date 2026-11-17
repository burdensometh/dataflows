class DataflowsException(Exception):
    pass


class ProcessorError(DataflowsException):

    def __init__(self, cause, *, processor_name, processor_object, processor_position):
        self.cause = cause
        self.processor_name = processor_name
        self.processor_object = processor_object
        self.processor_position = processor_position
        super().__init__(str(cause))

    def __str__(self):
        return 'Errored in processor %s in position #%s: %s' % \
                    (self.processor_name, self.processor_position, self.cause)


class SourceLoadError(DataflowsException):
    pass


class MergedSchemaError(DataflowsException):
    """Raised when a unified schema can't be determined for a merged load."""
    pass


class IncompatibleTypeError(MergedSchemaError):
    """Raised when a field holds values which can't be unified into a single type."""

    def __init__(self, field_name, types, origins=None):
        self.field_name = field_name
        self.types = list(types)
        self.origins = origins or {}
        details = ', '.join(
            '{} (in {})'.format(t, self.origins[t]) if t in self.origins else t
            for t in self.types
        )
        super().__init__(
            'Field {!r} has incompatible types across the merged inputs: {}'
            .format(field_name, details)
        )


class ExplicitFieldError(MergedSchemaError):
    """Raised when loaded data doesn't match an explicitly declared field."""
    pass
