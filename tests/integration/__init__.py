
splits_json = {
    "splitChange1_1": {
        "splits": [
            {"trafficTypeName": "user", "name": "SPLIT_2","trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779, "seed": -113875324, "status": "ACTIVE",
            "killed": False, "defaultTreatment": "off", "changeNumber": 1675443569027,
            "algo": 2, "configurations": {},
            "conditions": [
                {"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS","negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }]
            },{
            "trafficTypeName": "user", "name": "SPLIT_1", "trafficAllocation": 100, "trafficAllocationSeed": -1780071202,
            "seed": -1442762199, "status": "ACTIVE",
            "killed": False, "defaultTreatment": "off", "changeNumber": 1675443537882,
            "algo": 2, "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT", "matcherGroup": {"combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
                "label": "default rule"
                }]
            }
        ],
        "since": -1,
        "till": 1675443569027
    },
    "splitChange1_2": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": True,
            "defaultTreatment": "off",
            "changeNumber": 1675443767288,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": 1675443569027,
        "till": 1675443767288
    },
    "splitChange1_3": {
    "splits": [
        {
        "trafficTypeName": "user",
        "name": "SPLIT_1",
        "trafficAllocation": 100,
        "trafficAllocationSeed": -1780071202,
        "seed": -1442762199,
        "status": "ARCHIVED",
        "killed": False,
        "defaultTreatment": "off",
        "changeNumber": 1675443984594,
        "algo": 2,
        "configurations": {},
        "conditions": [
            {
            "conditionType": "ROLLOUT",
            "matcherGroup": {
                "combiner": "AND",
                "matchers": [
                {
                    "keySelector": { "trafficType": "user", "attribute": None },
                    "matcherType": "ALL_KEYS",
                    "negate": False,
                    "userDefinedSegmentMatcherData": None,
                    "whitelistMatcherData": None,
                    "unaryNumericMatcherData": None,
                    "betweenMatcherData": None,
                    "booleanMatcherData": None,
                    "dependencyMatcherData": None,
                    "stringMatcherData": None
                }
                ]
            },
            "partitions": [
                { "treatment": "on", "size": 0 },
                { "treatment": "off", "size": 100 }
            ],
            "label": "default rule"
            }
        ]
        },
        {
        "trafficTypeName": "user",
        "name": "SPLIT_2",
        "trafficAllocation": 100,
        "trafficAllocationSeed": 1057590779,
        "seed": -113875324,
        "status": "ACTIVE",
        "killed": False,
        "defaultTreatment": "off",
        "changeNumber": 1675443954220,
        "algo": 2,
        "configurations": {},
        "conditions": [
            {
            "conditionType": "ROLLOUT",
            "matcherGroup": {
                "combiner": "AND",
                "matchers": [
                {
                    "keySelector": { "trafficType": "user", "attribute": None },
                    "matcherType": "ALL_KEYS",
                    "negate": False,
                    "userDefinedSegmentMatcherData": None,
                    "whitelistMatcherData": None,
                    "unaryNumericMatcherData": None,
                    "betweenMatcherData": None,
                    "booleanMatcherData": None,
                    "dependencyMatcherData": None,
                    "stringMatcherData": None
                }
                ]
            },
            "partitions": [
                { "treatment": "on", "size": 100 },
                { "treatment": "off", "size": 0 }
            ],
            "label": "default rule"
            }
        ]
        }
    ],
    "since": 1675443767288,
    "till": 1675443984594
    },
    "splitChange2_1": {
        "splits": [
            {
            "name": "SPLIT_1",
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "configurations": {},
            "conditions": []
            }
        ]
    },
    "splitChange3_1": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443569027,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": -1,
        "till": 1675443569027
     },
    "splitChange3_2": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": True,
            "defaultTreatment": "off",
            "changeNumber": 1675443767288,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": 1675443569027,
        "till": 1675443569027
    },
    "splitChange4_1": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443569027,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            },
            {
            "trafficTypeName": "user",
            "name": "SPLIT_1",
            "trafficAllocation": 100,
            "trafficAllocationSeed": -1780071202,
            "seed": -1442762199,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443537882,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
                "label": "default rule"
                }
            ]
            }
        ]
    },
    "splitChange4_2": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": True,
            "defaultTreatment": "off",
            "changeNumber": 1675443767288,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ]
    },
    "splitChange4_3": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_1",
            "trafficAllocation": 100,
            "trafficAllocationSeed": -1780071202,
            "seed": -1442762199,
            "status": "ARCHIVED",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443984594,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
                "label": "default rule"
                }
            ]
            },
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443954220,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ]
    },
    "splitChange5_1": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443569027,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": -1,
        "till": 1675443569027
    },
    "splitChange5_2": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": True,
            "defaultTreatment": "off",
            "changeNumber": 1675443767288,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": 1675443569026,
        "till": 1675443569026
    },
    "splitChange6_1": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443569027,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            },
            {
            "trafficTypeName": "user",
            "name": "SPLIT_1",
            "trafficAllocation": 100,
            "trafficAllocationSeed": -1780071202,
            "seed": -1442762199,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443537882,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": -1,
        "till": -1
    },
    "splitChange6_2": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": True,
            "defaultTreatment": "off",
            "changeNumber": 1675443767288,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": -1,
        "till": -1
    },
    "splitChange6_3": {
        "splits": [
            {
            "trafficTypeName": "user",
            "name": "SPLIT_1",
            "trafficAllocation": 100,
            "trafficAllocationSeed": -1780071202,
            "seed": -1442762199,
            "status": "ARCHIVED",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443984594,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
                "label": "default rule"
                }
            ]
            },
            {
            "trafficTypeName": "user",
            "name": "SPLIT_2",
            "trafficAllocation": 100,
            "trafficAllocationSeed": 1057590779,
            "seed": -113875324,
            "status": "ACTIVE",
            "killed": False,
            "defaultTreatment": "off",
            "changeNumber": 1675443954220,
            "algo": 2,
            "configurations": {},
            "conditions": [
                {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                    "combiner": "AND",
                    "matchers": [
                    {
                        "keySelector": { "trafficType": "user", "attribute": None },
                        "matcherType": "ALL_KEYS",
                        "negate": False,
                        "userDefinedSegmentMatcherData": None,
                        "whitelistMatcherData": None,
                        "unaryNumericMatcherData": None,
                        "betweenMatcherData": None,
                        "booleanMatcherData": None,
                        "dependencyMatcherData": None,
                        "stringMatcherData": None
                    }
                    ]
                },
                "partitions": [
                    { "treatment": "on", "size": 100 },
                    { "treatment": "off", "size": 0 }
                ],
                "label": "default rule"
                }
            ]
            }
        ],
        "since": -1,
        "till": -1
    }
}
