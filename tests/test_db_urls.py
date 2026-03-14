from etl.common.db_urls import rewrite_tunneled_database_url


def test_rewrite_tunneled_database_url_infers_neon_endpoint_and_disables_channel_binding() -> None:
    raw = (
        "postgresql://user:pass@ep-sweet-sunset-ai8t67lb-pooler.c-4.us-east-1.aws.neon.tech/neondb"
        "?sslmode=require&channel_binding=require"
    )
    rewritten = rewrite_tunneled_database_url(raw, host="127.0.0.1", port=6543)
    assert rewritten.startswith("postgresql://user:pass@127.0.0.1:6543/neondb?")
    assert "sslmode=require" in rewritten
    assert "channel_binding=disable" in rewritten
    assert "options=endpoint%3Dep-sweet-sunset-ai8t67lb-pooler.c-4.us-east-1.aws.neon.tech" in rewritten


def test_rewrite_tunneled_database_url_preserves_existing_endpoint_option() -> None:
    raw = (
        "postgresql://user:pass@ep-sweet-sunset-ai8t67lb-pooler.c-4.us-east-1.aws.neon.tech/neondb"
        "?sslmode=require&options=endpoint%3Dcustom-endpoint"
    )
    rewritten = rewrite_tunneled_database_url(raw, host="127.0.0.1", port=6543)
    assert "options=endpoint%3Dcustom-endpoint" in rewritten
    assert "options=endpoint%3Dep-sweet-sunset-ai8t67lb-pooler.c-4.us-east-1.aws.neon.tech" not in rewritten
    assert "channel_binding=disable" in rewritten
