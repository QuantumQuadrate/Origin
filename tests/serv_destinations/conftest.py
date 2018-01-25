def pytest_addoption(parser):
    parser.addoption('--all', action='store_true', help='run all destinations')
    parser.addoption('--hdf5', action='store_true', help='run hdf5 destination')
    parser.addoption('--mysql', action='store_true', help='run mysql destination')
    parser.addoption('--filesystem', action='store_true', help='run filesystem destination')
    parser.addoption('--mongodb', action='store_true', help='run mongodb destination')


def pytest_generate_tests(metafunc):
    if 'destination' in metafunc.fixturenames:
        dests = ['hdf5', 'mysql', 'filesystem', 'mongodb']
        if metafunc.config.option.hdf5:
            dests = ['hdf5']
        if metafunc.config.option.mysql:
            dests = ['mysql']
        if metafunc.config.option.filesystem:
            dests = ['filesystem']
        if metafunc.config.option.mongodb:
            dests = ['mongodb']
        # but if all is specified add them all back
        if metafunc.config.option.all:
            dests = ['hdf5', 'mysql', 'filesystem', 'mongodb']
        metafunc.parametrize("destination", dests)
