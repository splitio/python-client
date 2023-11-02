split11 = {"splits": [{"trafficTypeName": "user", "name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779, "seed": -113875324, "status": "ACTIVE","killed": False, "defaultTreatment": "off", "changeNumber": 1675443569027,"algo": 2, "configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}], "sets": ["set2"]},{"trafficTypeName": "user", "name": "SPLIT_1", "trafficAllocation": 100, "trafficAllocationSeed": -1780071202,"seed": -1442762199, "status": "ACTIVE","killed": False, "defaultTreatment": "off", "changeNumber": 1675443537882,"algo": 2, "configurations": {},"conditions": [{"conditionType": "ROLLOUT", "matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 0 },{ "treatment": "off", "size": 100 }],"label": "default rule"}], "sets": ["set1"]}],"since": -1,"till": 1675443569027}
split12 = {"splits": [{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": True,"defaultTreatment": "off","changeNumber": 1675443767288,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}], "sets": ["set3"]}],"since": 1675443569027,"till": 167544376728}
split13 = {"splits": [{"trafficTypeName": "user","name": "SPLIT_1","trafficAllocation": 100,"trafficAllocationSeed": -1780071202,"seed": -1442762199,"status": "ARCHIVED","killed": False,"defaultTreatment": "off","changeNumber": 1675443984594,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 0 },{ "treatment": "off", "size": 100 }],"label": "default rule"}]},{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": False,"defaultTreatment": "off","changeNumber": 1675443954220,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}], "sets": ["set1", "set2"]}],"since": 1675443767288,"till": 1675443984594}

split41 = split11
split42 = split12
split43 = split13

split41["since"] = None
split41["till"] = None
split42["since"] = None
split42["till"] = None
split43["since"] = None
split43["till"] = None

split61 = split11
split62 = split12
split63 = split13

split61["since"] = -1
split61["till"] = -1
split62["since"] = -1
split62["till"] = -1
split63["since"] = -1
split63["till"] = -1

splits_json = {
    "splitChange1_1": split11,
    "splitChange1_2": split12,
    "splitChange1_3": split13,
    "splitChange2_1": {"splits": [{"name": "SPLIT_1","status": "ACTIVE","killed": False,"defaultTreatment": "off","configurations": {},"conditions": []}]},
    "splitChange3_1": {"splits": [{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": False,"defaultTreatment": "off","changeNumber": 1675443569027,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}]}],"since": -1,"till": 1675443569027},
    "splitChange3_2": {"splits": [{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": True,"defaultTreatment": "off","changeNumber": 1675443767288,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}]}],"since": 1675443569027,"till": 1675443569027},
    "splitChange4_1": split41,
    "splitChange4_2": split42,
    "splitChange4_3": split43,
    "splitChange5_1": {"splits": [{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": False,"defaultTreatment": "off","changeNumber": 1675443569027,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}]}],"since": -1,"till": 1675443569027},
    "splitChange5_2": {"splits": [{"trafficTypeName": "user","name": "SPLIT_2","trafficAllocation": 100,"trafficAllocationSeed": 1057590779,"seed": -113875324,"status": "ACTIVE","killed": True,"defaultTreatment": "off","changeNumber": 1675443767288,"algo": 2,"configurations": {},"conditions": [{"conditionType": "ROLLOUT","matcherGroup": {"combiner": "AND","matchers": [{"keySelector": { "trafficType": "user", "attribute": None },"matcherType": "ALL_KEYS","negate": False,"userDefinedSegmentMatcherData": None,"whitelistMatcherData": None,"unaryNumericMatcherData": None,"betweenMatcherData": None,"booleanMatcherData": None,"dependencyMatcherData": None,"stringMatcherData": None}]},"partitions": [{ "treatment": "on", "size": 100 },{ "treatment": "off", "size": 0 }],"label": "default rule"}]}],"since": 1675443569026,"till": 1675443569026},
    "splitChange6_1": split61,
    "splitChange6_2": split62,
    "splitChange6_3": split63,
}
