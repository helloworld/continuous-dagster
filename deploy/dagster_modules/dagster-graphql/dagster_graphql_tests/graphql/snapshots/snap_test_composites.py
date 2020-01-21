# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_composites 1'] = {
    'pipeline': {
        '__typename': 'Pipeline',
        'name': 'composites_pipeline',
        'solidHandles': [
            {
                'handleID': 'add_four',
                'solid': {
                    'definition': {
                        'inputMappings': [
                            {
                                'definition': {
                                    'name': 'num'
                                },
                                'mappedInput': {
                                    'definition': {
                                        'name': 'num'
                                    },
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            }
                        ],
                        'outputMappings': [
                            {
                                'definition': {
                                    'name': 'result'
                                },
                                'mappedOutput': {
                                    'definition': {
                                        'name': 'result'
                                    },
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            }
                        ]
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                            ]
                        }
                    ],
                    'name': 'add_four',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                                {
                                    'solid': {
                                        'name': 'div_four'
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_1',
                'solid': {
                    'definition': {
                        'inputMappings': [
                            {
                                'definition': {
                                    'name': 'num'
                                },
                                'mappedInput': {
                                    'definition': {
                                        'name': 'num'
                                    },
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            }
                        ],
                        'outputMappings': [
                            {
                                'definition': {
                                    'name': 'result'
                                },
                                'mappedOutput': {
                                    'definition': {
                                        'name': 'result'
                                    },
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            }
                        ]
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                            ]
                        }
                    ],
                    'name': 'adder_1',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                                {
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_1.adder_1',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                            ]
                        }
                    ],
                    'name': 'adder_1',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                                {
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_1.adder_2',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                                {
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            ]
                        }
                    ],
                    'name': 'adder_2',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_2',
                'solid': {
                    'definition': {
                        'inputMappings': [
                            {
                                'definition': {
                                    'name': 'num'
                                },
                                'mappedInput': {
                                    'definition': {
                                        'name': 'num'
                                    },
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            }
                        ],
                        'outputMappings': [
                            {
                                'definition': {
                                    'name': 'result'
                                },
                                'mappedOutput': {
                                    'definition': {
                                        'name': 'result'
                                    },
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            }
                        ]
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                                {
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            ]
                        }
                    ],
                    'name': 'adder_2',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_2.adder_1',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                            ]
                        }
                    ],
                    'name': 'adder_1',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                                {
                                    'solid': {
                                        'name': 'adder_2'
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'add_four.adder_2.adder_2',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                                {
                                    'solid': {
                                        'name': 'adder_1'
                                    }
                                }
                            ]
                        }
                    ],
                    'name': 'adder_2',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'div_four',
                'solid': {
                    'definition': {
                        'inputMappings': [
                            {
                                'definition': {
                                    'name': 'num'
                                },
                                'mappedInput': {
                                    'definition': {
                                        'name': 'num'
                                    },
                                    'solid': {
                                        'name': 'div_1'
                                    }
                                }
                            }
                        ],
                        'outputMappings': [
                            {
                                'definition': {
                                    'name': 'result'
                                },
                                'mappedOutput': {
                                    'definition': {
                                        'name': 'result'
                                    },
                                    'solid': {
                                        'name': 'div_2'
                                    }
                                }
                            }
                        ]
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                                {
                                    'solid': {
                                        'name': 'add_four'
                                    }
                                }
                            ]
                        }
                    ],
                    'name': 'div_four',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'div_four.div_1',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                            ]
                        }
                    ],
                    'name': 'div_1',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                                {
                                    'solid': {
                                        'name': 'div_2'
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                'handleID': 'div_four.div_2',
                'solid': {
                    'definition': {
                    },
                    'inputs': [
                        {
                            'definition': {
                                'name': 'num'
                            },
                            'dependsOn': [
                                {
                                    'solid': {
                                        'name': 'div_1'
                                    }
                                }
                            ]
                        }
                    ],
                    'name': 'div_2',
                    'outputs': [
                        {
                            'definition': {
                                'name': 'result'
                            },
                            'dependedBy': [
                            ]
                        }
                    ]
                }
            }
        ]
    }
}
