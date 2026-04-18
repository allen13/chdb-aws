from chdb_aws.main import main


def test_main_prints_greeting(capsys):
    main()
    captured = capsys.readouterr()
    assert "Hello from chdb-aws!" in captured.out
