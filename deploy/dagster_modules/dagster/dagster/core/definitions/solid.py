from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

import six

from dagster import check
from dagster.config.field_utils import check_user_facing_opt_config_param
from dagster.core.definitions.config import ConfigMapping
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.utils import frozendict, frozenlist

from .container import IContainSolids, create_execution_structure, validate_dependency_dict
from .dependency import SolidHandle
from .input import InputDefinition, InputMapping
from .output import OutputDefinition, OutputMapping
from .utils import check_valid_name


class ISolidDefinition(six.with_metaclass(ABCMeta)):
    def __init__(self, name, input_defs, output_defs, description=None, metadata=None):
        self._name = check_valid_name(name)
        self._description = check.opt_str_param(description, 'description')
        self._metadata = check.opt_dict_param(metadata, 'metadata', key_type=str)
        self._input_defs = frozenlist(input_defs)
        self._input_dict = frozendict({input_def.name: input_def for input_def in input_defs})
        check.invariant(len(self._input_defs) == len(self._input_dict), 'Duplicate input def names')
        self._output_defs = frozenlist(output_defs)
        self._output_dict = frozendict({output_def.name: output_def for output_def in output_defs})
        check.invariant(
            len(self._output_defs) == len(self._output_dict), 'Duplicate output def names'
        )

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def metadata(self):
        return self._metadata

    @property
    def input_defs(self):
        return self._input_defs

    @property
    def input_dict(self):
        return self._input_dict

    @property
    def output_defs(self):
        return self._output_defs

    @property
    def output_dict(self):
        return self._output_dict

    def has_input(self, name):
        check.str_param(name, 'name')
        return name in self._input_dict

    def input_def_named(self, name):
        check.str_param(name, 'name')
        return self._input_dict[name]

    def has_output(self, name):
        check.str_param(name, 'name')
        return name in self._output_dict

    def output_def_named(self, name):
        check.str_param(name, 'name')
        return self._output_dict[name]

    @property
    def has_configurable_inputs(self):
        return any([inp.runtime_type.input_hydration_config for inp in self._input_defs])

    @property
    def has_configurable_outputs(self):
        return any([out.runtime_type.output_materialization_config for out in self._output_defs])

    @abstractproperty
    def has_config_entry(self):
        raise NotImplementedError()

    @abstractmethod
    def iterate_solid_defs(self):
        raise NotImplementedError()

    @abstractmethod
    def resolve_output_to_origin(self, output_name, handle):
        raise NotImplementedError()

    def all_input_output_types(self):
        for input_def in self._input_defs:
            yield input_def.runtime_type
            for inner_type in input_def.runtime_type.inner_types:
                yield inner_type

        for output_def in self._output_defs:
            yield output_def.runtime_type
            for inner_type in output_def.runtime_type.inner_types:
                yield inner_type

    def __call__(self, *args, **kwargs):
        from .composition import CallableSolidNode

        return CallableSolidNode(self)(*args, **kwargs)

    def alias(self, name):
        from .composition import CallableSolidNode

        return CallableSolidNode(self, name)


class SolidDefinition(ISolidDefinition):
    '''
    The definition of a Solid that peforms a user defined computation.

    For more details on what a solid is, refer to the
    `Solid Guide <../../learn/guides/solid/solid.html>`_ .

    End users should prefer the :func:`@solid <solid>` and :func:`@lambda_solid <lambda_solid>`
    decorators. SolidDefinition is generally intended to be used by framework authors.

    Args:
        name (str): Name of the solid. Must be unique within any :py:class:`PipelineDefinition`
            using the solid.
        input_defs (List[InputDefinition]): Inputs of the solid.
        compute_fn (Callable): The core of the solid, the function that does the actual
            computation. The signature of this function is determined by ``input_defs``, with
            an additional injected first argument, ``context``, a collection of information provided
            by the system.

            This function must return a generator, which must yield one :py:class:`Output` for each
            of the solid's ``output_defs``, and additionally may yield other types of Dagster
            events, including :py:class:`Materialization` and :py:class:`ExpectationResult`.
        output_defs (List[OutputDefinition]): Outputs of the solid.
        config (Optional[Any]): The schema for the config. Configuration data available in
            `init_context.solid_config`.
            This value can be a:
                - :py:class:`Field`
                - Python primitive types that resolve to dagster config types
                    - int, float, bool, str, list.
                - A dagster config type: Int, Float, Bool, List, Optional, :py:class:`Selector`, :py:class:`Dict`
                - A bare python dictionary, which is wrapped in Field(Dict(...)). Any values of
                in the dictionary get resolved by the same rules, recursively.
        description (Optional[str]): Human-readable description of the solid.
        metadata (Optional[Dict[Any, Any]]): Arbitrary metadata for the solid. Frameworks may
            expect and require certain metadata to be attached to a solid.
        required_resource_keys (Optional[Set[str]]): Set of resources handles required by this
            solid.

    Examples:
        .. code-block:: python

            def _add_one(_context, inputs):
                yield Output(inputs["num"] + 1)

            SolidDefinition(
                name="add_one",
                input_defs=[InputDefinition("num", Int)],
                output_defs=[OutputDefinition(Int)], # default name ("result")
                compute_fn=_add_one,
            )
    '''

    def __init__(
        self,
        name,
        input_defs,
        compute_fn,
        output_defs,
        config=None,
        description=None,
        metadata=None,
        required_resource_keys=None,
        step_metadata_fn=None,
    ):
        self._compute_fn = check.callable_param(compute_fn, 'compute_fn')
        self._config_field = check_user_facing_opt_config_param(config, 'config',)
        self._required_resource_keys = check.opt_set_param(
            required_resource_keys, 'required_resource_keys', of_type=str
        )
        self._step_metadata_fn = check.opt_callable_param(step_metadata_fn, 'step_metadata_fn')

        super(SolidDefinition, self).__init__(
            name=name,
            input_defs=check.list_param(input_defs, 'input_defs', InputDefinition),
            output_defs=check.list_param(output_defs, 'output_defs', OutputDefinition),
            description=description,
            metadata=metadata,
        )

    @property
    def compute_fn(self):
        return self._compute_fn

    @property
    def config_field(self):
        return self._config_field

    @property
    def required_resource_keys(self):
        return self._required_resource_keys

    @property
    def step_metadata_fn(self):
        return self._step_metadata_fn

    @property
    def has_config_entry(self):
        return self._config_field or self.has_configurable_inputs or self.has_configurable_outputs

    def all_runtime_types(self):
        for tt in self.all_input_output_types():
            yield tt

    def iterate_solid_defs(self):
        yield self

    def resolve_output_to_origin(self, output_name, handle):
        return self.output_def_named(output_name), handle


class CompositeSolidDefinition(ISolidDefinition, IContainSolids):
    '''The core unit of composition and abstraction, composite solids allow you to
    define a solid from a graph of solids.

    In the same way you would refactor a block of code in to a function to deduplicate, organize,
    or manage complexity - you can refactor solids in a pipeline in to a composite solid.

    Args:
        name (str): The name of this composite solid. Must be unique within any
            :py:class:`PipelineDefinition` using the solid.
        solid_defs (List[Union[SolidDefinition, CompositeSolidDefinition]]): The set of solid
            definitions used in this composite solid. Composites may be arbitrarily nested.
        input_mappings (Optional[List[InputMapping]]): Define the inputs to the composite solid,
            and how they map to the inputs of its constituent solids.
        output_mappings (Optional[List[OutputMapping]]): Define the outputs of the composite solid,
            and how they map from the outputs of its constituent solids.
        config_mapping (Optional[ConfigMapping]): By specifying a config mapping, you can override
            the configuration for the child solids contained within this composite solid. Config
            mappings require both a configuration field to be specified, which is exposed as the
            configuration for the composite solid, and a configuration mapping function, which
            is called to map the configuration of the composite solid into the configuration that
            is applied to any child solids.
        dependencies (Optional[Dict[Union[str, SolidInvocation], Dict[str, DependencyDefinition]]]):
            A structure that declares where each solid gets its inputs. The keys at the top
            level dict are either string names of solids or SolidInvocations. The values
            are dicts that map input names to DependencyDefinitions.
        description (Optional[str]): Human readable description of this composite solid.
        metadata (Optional[Dict[Any, Any]]): Arbitrary metadata for the composite solid. Frameworks
            may expect and require certain metadata to be attached to a solid.

    Examples:

        .. code-block:: python

            @lambda_solid
            def add_one(num: int) -> int:
                return num + 1

            add_two = CompositeSolidDefinition(
                'add_two',
                solid_defs=[add_one],
                dependencies={
                    SolidInvocation('add_one', 'adder_1'): {},
                    SolidInvocation('add_one', 'adder_2'): {'num': DependencyDefinition('adder_1')},
                },
                input_mappings=[InputDefinition('num', Int).mapping_to('adder_1', 'num')],
                output_mappings=[OutputDefinition(Int).mapping_from('adder_2')],
            )
    '''

    def __init__(
        self,
        name,
        solid_defs,
        input_mappings=None,
        output_mappings=None,
        config_mapping=None,
        dependencies=None,
        description=None,
        metadata=None,
    ):
        check.str_param(name, 'name')
        self._solid_defs = check.list_param(solid_defs, 'solid_defs', of_type=ISolidDefinition)

        self._dependencies = validate_dependency_dict(dependencies)
        dependency_structure, solid_dict = create_execution_structure(
            solid_defs, self._dependencies, container_definition=self
        )

        # List[InputMapping]
        self._input_mappings, input_def_list = _validate_in_mappings(
            check.opt_list_param(input_mappings, 'input_mappings'), solid_dict, name
        )
        # List[OutputMapping]
        self._output_mappings = _validate_out_mappings(
            check.opt_list_param(output_mappings, 'output_mappings'), solid_dict, name
        )

        self._config_mapping = check.opt_inst_param(config_mapping, 'config_mapping', ConfigMapping)
        self._solid_dict = solid_dict
        self._dependency_structure = dependency_structure

        output_defs = [output_mapping.definition for output_mapping in self._output_mappings]

        super(CompositeSolidDefinition, self).__init__(
            name=name,
            input_defs=input_def_list,
            output_defs=output_defs,
            description=description,
            metadata=metadata,
        )

    @property
    def input_mappings(self):
        return self._input_mappings

    @property
    def output_mappings(self):
        return self._output_mappings

    @property
    def config_mapping(self):
        return self._config_mapping

    @property
    def dependencies(self):
        return self._dependencies

    def iterate_solid_defs(self):
        yield self
        for outer_solid_def in self._solid_defs:
            for solid_def in outer_solid_def.iterate_solid_defs():
                yield solid_def

    @property
    def solids(self):
        return list(self._solid_dict.values())

    def solid_named(self, name):
        return self._solid_dict[name]

    def has_solid_named(self, name):
        check.str_param(name, 'name')
        return name in self._solid_dict

    def get_solid(self, handle):
        check.inst_param(handle, 'handle', SolidHandle)
        current = handle
        lineage = []
        while current:
            lineage.append(current.name)
            current = current.parent

        name = lineage.pop()
        solid = self.solid_named(name)
        while lineage:
            name = lineage.pop()
            solid = solid.definition.solid_named(name)

        return solid

    @property
    def dependency_structure(self):
        return self._dependency_structure

    @property
    def required_resource_keys(self):
        required_resource_keys = set()
        for solid in self.solids:
            required_resource_keys.update(solid.definition.required_resource_keys)

        return required_resource_keys

    @property
    def has_config_mapping(self):
        return self._config_mapping is not None

    @property
    def has_descendant_config_mapping(self):
        return any(
            (
                isinstance(solid, CompositeSolidDefinition) and solid.has_config_mapping
                for solid in self.iterate_solid_defs()
            )
        )

    @property
    def has_config_entry(self):
        has_solid_config = any([solid.definition.has_config_entry for solid in self.solids])
        return (
            self.has_config_mapping
            or has_solid_config
            or self.has_configurable_inputs
            or self.has_configurable_outputs
        )

    def mapped_input(self, solid_name, input_name):
        for mapping in self._input_mappings:
            if mapping.solid_name == solid_name and mapping.input_name == input_name:
                return mapping
        return None

    def get_output_mapping(self, output_name):
        for mapping in self._output_mappings:
            if mapping.definition.name == output_name:
                return mapping
        return None

    def resolve_output_to_origin(self, output_name, handle):
        mapping = self.get_output_mapping(output_name)
        check.invariant(mapping, 'Can only resolve outputs for valid output names')
        mapped_solid = self.solid_named(mapping.solid_name)
        return mapped_solid.definition.resolve_output_to_origin(
            mapping.output_name,
            SolidHandle(mapped_solid.name, mapped_solid.definition.name, handle),
        )

    def all_runtime_types(self):
        for tt in self.all_input_output_types():
            yield tt

        for solid_def in self._solid_defs:
            for ttype in solid_def.all_runtime_types():
                yield ttype


def _validate_in_mappings(input_mappings, solid_dict, name):
    input_def_dict = OrderedDict()
    for mapping in input_mappings:
        if isinstance(mapping, InputMapping):
            if input_def_dict.get(mapping.definition.name):
                if input_def_dict[mapping.definition.name] != mapping.definition:
                    raise DagsterInvalidDefinitionError(
                        'In CompositeSolid {name} multiple input mappings with same '
                        'definition name but different definitions'.format(name=name),
                    )
            else:
                input_def_dict[mapping.definition.name] = mapping.definition

            target_solid = solid_dict.get(mapping.solid_name)
            if target_solid is None:
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid '{name}' input mapping references solid "
                    "'{solid_name}' which it does not contain.".format(
                        name=name, solid_name=mapping.solid_name
                    )
                )
            if not target_solid.has_input(mapping.input_name):
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid '{name}' input mapping to solid '{mapping.solid_name}' "
                    "which contains no input named '{mapping.input_name}'".format(
                        name=name, mapping=mapping
                    )
                )

            target_input = target_solid.input_def_named(mapping.input_name)

            if target_input.runtime_type != mapping.definition.runtime_type:
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid '{name}' input "
                    "'{mapping.definition.name}' of type {mapping.definition.runtime_type.display_name} maps to "
                    "{mapping.solid_name}.{mapping.input_name} of different type "
                    "{target_input.runtime_type.display_name}. InputMapping source and "
                    "destination must have the same type.".format(
                        mapping=mapping, name=name, target_input=target_input
                    )
                )

        elif isinstance(mapping, InputDefinition):
            raise DagsterInvalidDefinitionError(
                "In CompositeSolid '{name}' you passed an InputDefinition "
                "named '{input_name}' directly in to input_mappings. Return "
                "an InputMapping by calling mapping_to on the InputDefinition.".format(
                    name=name, input_name=mapping.name,
                )
            )
        else:
            raise DagsterInvalidDefinitionError(
                "In CompositeSolid '{name}' received unexpected type '{type}' in input_mappings. "
                "Provide an OutputMapping using InputDefinition(...).mapping_to(...)".format(
                    type=type(mapping), name=name,
                )
            )

    return input_mappings, input_def_dict.values()


def _validate_out_mappings(output_mappings, solid_dict, name):
    for mapping in output_mappings:
        if isinstance(mapping, OutputMapping):

            target_solid = solid_dict.get(mapping.solid_name)
            if target_solid is None:
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid '{name}' output mapping references solid "
                    "'{solid_name}' which it does not contain.".format(
                        name=name, solid_name=mapping.solid_name
                    )
                )
            if not target_solid.has_output(mapping.output_name):
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid {name} output mapping from solid '{mapping.solid_name}' "
                    "which contains no output named '{mapping.output_name}'".format(
                        name=name, mapping=mapping
                    )
                )

            target_output = target_solid.output_def_named(mapping.output_name)

            if target_output.runtime_type != mapping.definition.runtime_type:
                raise DagsterInvalidDefinitionError(
                    "In CompositeSolid '{name}' output "
                    "'{mapping.definition.name}' of type {mapping.definition.runtime_type.display_name} "
                    "maps from {mapping.solid_name}.{mapping.output_name} of different type "
                    "{target_output.runtime_type.display_name}. OutputMapping source "
                    "and destination must have the same type.".format(
                        mapping=mapping, name=name, target_output=target_output
                    )
                )

        elif isinstance(mapping, OutputDefinition):
            raise DagsterInvalidDefinitionError(
                "You passed an OutputDefinition named '{output_name}' directly "
                "in to output_mappings. Return an OutputMapping by calling "
                "mapping_from on the OutputDefinition.".format(output_name=mapping.name)
            )
        else:
            raise DagsterInvalidDefinitionError(
                "Received unexpected type '{type}' in output_mappings. "
                "Provide an OutputMapping using OutputDefinition(...).mapping_from(...)".format(
                    type=type(mapping)
                )
            )
    return output_mappings
