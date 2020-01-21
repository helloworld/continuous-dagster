import json

from dagster.utils import file_relative_path

from .utils import sync_execute_get_events


def get_expectation_results(logs, solid_name):
    def _f():
        for log in logs:
            if (
                log['__typename'] == 'StepExpectationResultEvent'
                and log['step']['solidHandleID'] == solid_name
            ):
                yield log

    return list(_f())


def get_expectation_result(logs, solid_name):
    expt_results = get_expectation_results(logs, solid_name)
    if len(expt_results) != 1:
        raise Exception('Only expected one expectation result')
    return expt_results[0]


def sanitize(res):
    if isinstance(res, list):
        for i in range(len(res)):
            res[i] = sanitize(res[i])
        return res

    for k, v in res.items():
        if k == 'timestamp':
            res[k] = '*************'
        if k == 'runId':
            res[k] = '*******-****-****-****-************'
        if isinstance(v, dict):
            res[k] = sanitize(v)

    return res


def test_basic_expectations_within_compute_step_events(snapshot):
    logs = sync_execute_get_events(
        variables={
            'executionParams': {
                'selector': {'name': 'pipeline_with_expectations'},
                'mode': 'default',
            }
        }
    )

    emit_failed_expectation_event = get_expectation_result(logs, 'emit_failed_expectation')
    assert emit_failed_expectation_event['expectationResult']['success'] is False
    assert emit_failed_expectation_event['expectationResult']['description'] == 'Failure'
    failed_result_metadata = json.loads(
        emit_failed_expectation_event['expectationResult']['metadataEntries'][0]['jsonString']
    )
    assert emit_failed_expectation_event['expectationResult']['label'] == 'always_false'

    assert failed_result_metadata == {'reason': 'Relentless pessimism.'}

    emit_successful_expectation_event = get_expectation_result(logs, 'emit_successful_expectation')

    assert emit_successful_expectation_event['expectationResult']['success'] is True
    assert emit_successful_expectation_event['expectationResult']['description'] == 'Successful'
    assert emit_successful_expectation_event['expectationResult']['label'] == 'always_true'
    successful_result_metadata = json.loads(
        emit_successful_expectation_event['expectationResult']['metadataEntries'][0]['jsonString']
    )

    assert successful_result_metadata == {'reason': 'Just because.'}

    emit_no_metadata = get_expectation_result(logs, 'emit_successful_expectation_no_metadata')
    assert not emit_no_metadata['expectationResult']['metadataEntries']

    snapshot.assert_match(sanitize(get_expectation_results(logs, 'emit_failed_expectation')))
    snapshot.assert_match(sanitize(get_expectation_results(logs, 'emit_successful_expectation')))
    snapshot.assert_match(
        sanitize(get_expectation_results(logs, 'emit_successful_expectation_no_metadata'))
    )


def test_basic_input_output_expectations(snapshot):
    logs = sync_execute_get_events(
        variables={
            'executionParams': {
                'selector': {'name': 'csv_hello_world_with_expectations'},
                'environmentConfigData': {
                    'solids': {
                        'sum_solid': {
                            'inputs': {'num': file_relative_path(__file__, '../data/num.csv')}
                        }
                    }
                },
                'mode': 'default',
            }
        }
    )

    expectation_results = get_expectation_results(logs, 'df_expectations_solid')
    assert len(expectation_results) == 2

    snapshot.assert_match(sanitize(expectation_results))
