# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_query_all_solids 1'] = {
    'usedSolids': [
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'a_solid_with_multilayered_config'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'more_complicated_nested_config'
                    },
                    'solidHandle': {
                        'handleID': 'a_solid_with_multilayered_config'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'a_solid_with_three_field_config'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'more_complicated_config'
                    },
                    'solidHandle': {
                        'handleID': 'a_solid_with_three_field_config'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'add_four'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'add_one'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_1.adder_1'
                    }
                },
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_2.adder_1'
                    }
                },
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_1.adder_2'
                    }
                },
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_2.adder_2'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'add_two'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_1'
                    }
                },
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'add_four.adder_2'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'apply_to_three'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'multi_mode_with_resources'
                    },
                    'solidHandle': {
                        'handleID': 'apply_to_three'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'df_expectations_solid'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'csv_hello_world_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'df_expectations_solid'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'div_four'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'div_four'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'div_two'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'div_four.div_1'
                    }
                },
                {
                    'pipeline': {
                        'name': 'composites_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'div_four.div_2'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'emit_failed_expectation'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'emit_failed_expectation'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'emit_successful_expectation'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'emit_successful_expectation'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'emit_successful_expectation_no_metadata'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'emit_successful_expectation_no_metadata'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'fail'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'eventually_successful'
                    },
                    'solidHandle': {
                        'handleID': 'fail'
                    }
                },
                {
                    'pipeline': {
                        'name': 'eventually_successful'
                    },
                    'solidHandle': {
                        'handleID': 'fail_2'
                    }
                },
                {
                    'pipeline': {
                        'name': 'eventually_successful'
                    },
                    'solidHandle': {
                        'handleID': 'fail_3'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'loop'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'infinite_loop_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'loop'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'materialize'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'materialization_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'materialize'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'noop_solid'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'noop_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'noop_solid'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'reset'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'eventually_successful'
                    },
                    'solidHandle': {
                        'handleID': 'reset'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_any'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'scalar_output_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'return_any'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_bool'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'scalar_output_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'return_bool'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_hello'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'no_config_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'return_hello'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_int'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'scalar_output_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'return_int'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_six'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'multi_mode_with_loggers'
                    },
                    'solidHandle': {
                        'handleID': 'return_six'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'return_str'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'scalar_output_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'return_str'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'solid_metadata_creation'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_step_metadata'
                    },
                    'solidHandle': {
                        'handleID': 'solid_metadata_creation'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'solid_with_list'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_list'
                    },
                    'solidHandle': {
                        'handleID': 'solid_with_list'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'solid_with_required_resource'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'required_resource_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'solid_with_required_resource'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'spawn'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'eventually_successful'
                    },
                    'solidHandle': {
                        'handleID': 'spawn'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'spew'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'spew_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'spew'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'sum_solid'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'csv_hello_world'
                    },
                    'solidHandle': {
                        'handleID': 'sum_solid'
                    }
                },
                {
                    'pipeline': {
                        'name': 'csv_hello_world_df_input'
                    },
                    'solidHandle': {
                        'handleID': 'sum_solid'
                    }
                },
                {
                    'pipeline': {
                        'name': 'csv_hello_world_two'
                    },
                    'solidHandle': {
                        'handleID': 'sum_solid'
                    }
                },
                {
                    'pipeline': {
                        'name': 'csv_hello_world_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'sum_solid'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'sum_sq_solid'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'csv_hello_world'
                    },
                    'solidHandle': {
                        'handleID': 'sum_sq_solid'
                    }
                },
                {
                    'pipeline': {
                        'name': 'csv_hello_world_df_input'
                    },
                    'solidHandle': {
                        'handleID': 'sum_sq_solid'
                    }
                },
                {
                    'pipeline': {
                        'name': 'csv_hello_world_with_expectations'
                    },
                    'solidHandle': {
                        'handleID': 'sum_sq_solid'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'takes_an_enum'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'pipeline_with_enum_config'
                    },
                    'solidHandle': {
                        'handleID': 'takes_an_enum'
                    }
                }
            ]
        },
        {
            '__typename': 'UsedSolid',
            'definition': {
                'name': 'throw_a_thing'
            },
            'invocations': [
                {
                    'pipeline': {
                        'name': 'naughty_programmer_pipeline'
                    },
                    'solidHandle': {
                        'handleID': 'throw_a_thing'
                    }
                }
            ]
        }
    ]
}
