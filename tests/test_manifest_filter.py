from databricks_dbt_factory.ManifestFilter import ManifestFilter


def _model(package, name, *, fqn=None, tags=None, path=None, depends_on=None):
    full = f"model.{package}.{name}"
    return full, {
        'resource_type': 'model',
        'name': name,
        'package_name': package,
        'fqn': fqn or [package, name],
        'tags': tags or [],
        'original_file_path': path or f"models/{name}.sql",
        'depends_on': {'nodes': depends_on or []},
    }


def _test(package, name, depends_on, severity='error'):
    full = f"test.{package}.{name}"
    return full, {
        'resource_type': 'test',
        'name': name,
        'package_name': package,
        'fqn': [package, name],
        'depends_on': {'nodes': depends_on},
        'config': {'severity': severity},
    }


def _unit_test(package, model, name):
    full = f"unit_test.{package}.{model}.{name}"
    return full, {
        'resource_type': 'unit_test',
        'name': name,
        'model': model,
        'package_name': package,
        'fqn': [package, model, name],
        'depends_on': {'nodes': [f"model.{package}.{model}"]},
    }


def _manifest(nodes, unit_tests=None, parent_map=None, child_map=None):
    return {
        'nodes': dict(nodes),
        'sources': {},
        'unit_tests': dict(unit_tests or {}),
        'parent_map': parent_map or {},
        'child_map': child_map or {},
        # a passthrough key the filter must leave untouched
        'metadata': {'dbt_version': '1.8.7'},
    }


def _model_keys(filtered):
    return {uid for uid, info in filtered['nodes'].items() if info['resource_type'] == 'model'}


def test_tag_selector_keeps_only_tagged_models():
    manifest = _manifest(
        [
            _model('pkg', 'a', tags=['daily']),
            _model('pkg', 'b', tags=['weekly']),
            _model('pkg', 'c', tags=['daily', 'core']),
        ]
    )

    filtered = ManifestFilter('tag:daily').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.a', 'model.pkg.c'}
    # passthrough metadata preserved
    assert filtered['metadata'] == {'dbt_version': '1.8.7'}


def test_config_tags_are_matched_too():
    _, info = _model('pkg', 'a')
    info['tags'] = []
    info['config'] = {'tags': ['nightly']}
    manifest = _manifest([('model.pkg.a', info), _model('pkg', 'b')])

    filtered = ManifestFilter('tag:nightly').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.a'}


def test_path_selector_matches_directory_prefix():
    manifest = _manifest(
        [
            _model('pkg', 'stg_orders', path='models/staging/stg_orders.sql'),
            _model('pkg', 'stg_users', path='models/staging/stg_users.sql'),
            _model('pkg', 'mart_sales', path='models/marts/mart_sales.sql'),
        ]
    )

    filtered = ManifestFilter('path:models/staging').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.stg_orders', 'model.pkg.stg_users'}


def test_bare_name_and_fqn_selectors():
    manifest = _manifest([_model('pkg', 'orders', fqn=['pkg', 'staging', 'orders']), _model('pkg', 'users')])

    assert _model_keys(ManifestFilter('orders').apply(manifest)) == {'model.pkg.orders'}
    assert _model_keys(ManifestFilter('pkg.staging.orders').apply(manifest)) == {'model.pkg.orders'}
    # fqn path prefix selects the subtree
    assert _model_keys(ManifestFilter('pkg.staging').apply(manifest)) == {'model.pkg.orders'}


def test_space_separated_selectors_are_unioned():
    manifest = _manifest(
        [
            _model('pkg', 'a', tags=['x']),
            _model('pkg', 'b', tags=['y']),
            _model('pkg', 'c', tags=['z']),
        ]
    )

    filtered = ManifestFilter('tag:x tag:z').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.a', 'model.pkg.c'}


def test_descendants_operator_includes_downstream():
    # a -> b -> c
    manifest = _manifest(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b', depends_on=['model.pkg.a']),
            _model('pkg', 'c', depends_on=['model.pkg.b']),
        ],
        child_map={'model.pkg.a': ['model.pkg.b'], 'model.pkg.b': ['model.pkg.c'], 'model.pkg.c': []},
        parent_map={'model.pkg.a': [], 'model.pkg.b': ['model.pkg.a'], 'model.pkg.c': ['model.pkg.b']},
    )

    filtered = ManifestFilter('a+').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.a', 'model.pkg.b', 'model.pkg.c'}


def test_ancestors_operator_includes_upstream():
    manifest = _manifest(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b', depends_on=['model.pkg.a']),
            _model('pkg', 'c', depends_on=['model.pkg.b']),
        ],
        child_map={'model.pkg.a': ['model.pkg.b'], 'model.pkg.b': ['model.pkg.c'], 'model.pkg.c': []},
        parent_map={'model.pkg.a': [], 'model.pkg.b': ['model.pkg.a'], 'model.pkg.c': ['model.pkg.b']},
    )

    filtered = ManifestFilter('+c').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.a', 'model.pkg.b', 'model.pkg.c'}


def test_deps_are_pruned_to_selected_set():
    # Select only b (downstream of a). b's depends_on to the dropped a must be pruned so its
    # generated task does not gate on a node that no longer exists.
    manifest = _manifest(
        [
            _model('pkg', 'a'),
            _model('pkg', 'b', depends_on=['model.pkg.a']),
        ]
    )

    filtered = ManifestFilter('b').apply(manifest)

    assert _model_keys(filtered) == {'model.pkg.b'}
    assert filtered['nodes']['model.pkg.b']['depends_on']['nodes'] == []


def test_tests_of_dropped_models_are_removed():
    manifest = _manifest(
        [_model('pkg', 'a', tags=['keep']), _model('pkg', 'b')],
        [],
    )
    test_key, test_info = _test('pkg', 'unique_b', ['model.pkg.b'])
    manifest['nodes'][test_key] = test_info

    filtered = ManifestFilter('tag:keep').apply(manifest)

    # b and its test are dropped; only a survives
    assert _model_keys(filtered) == {'model.pkg.a'}
    assert test_key not in filtered['nodes']


def test_unit_tests_follow_their_model():
    manifest = _manifest(
        [_model('pkg', 'a', tags=['keep']), _model('pkg', 'b')],
        [_unit_test('pkg', 'a', 'ut_a'), _unit_test('pkg', 'b', 'ut_b')],
    )

    filtered = ManifestFilter('tag:keep').apply(manifest)

    assert set(filtered['unit_tests']) == {'unit_test.pkg.a.ut_a'}


def test_no_match_yields_empty_selection():
    manifest = _manifest([_model('pkg', 'a', tags=['x'])])

    filtered = ManifestFilter('tag:does_not_exist').apply(manifest)

    assert _model_keys(filtered) == set()
    assert not filtered['nodes']
