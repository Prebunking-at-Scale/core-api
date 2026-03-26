"""Tests for multi-channel alert notifications (email + Slack)"""
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from core.alerts.models import Alert, AlertScope, AlertType, AlertTriggered, ChannelConfig, ChannelType
from core.alerts.repo import AlertRepository
from core.alerts.service import AlertService


@pytest.fixture
def alert_with_email_only():
    """Alert configured for email only"""
    return Alert(
        id=uuid4(),
        user_id=uuid4(),
        organisation_id=uuid4(),
        name="Test Email Alert",
        alert_type=AlertType.NARRATIVE_VIEWS,
        scope=AlertScope.GENERAL,
        threshold=1000,
        enabled=True,
        channels=[{"channel_type": "email"}]
    )


@pytest.fixture
def alert_with_slack_only():
    """Alert configured for Slack only"""
    return Alert(
        id=uuid4(),
        user_id=uuid4(),
        organisation_id=uuid4(),
        name="Test Slack Alert",
        alert_type=AlertType.NARRATIVE_VIEWS,
        scope=AlertScope.GENERAL,
        threshold=1000,
        enabled=True,
        channels=[{"channel_type": "slack", "slack_channel_id": "C12345678"}]
    )


@pytest.fixture
def alert_with_both_channels():
    """Alert configured for both email and Slack"""
    return Alert(
        id=uuid4(),
        user_id=uuid4(),
        organisation_id=uuid4(),
        name="Test Multi-Channel Alert",
        alert_type=AlertType.NARRATIVE_VIEWS,
        scope=AlertScope.GENERAL,
        threshold=1000,
        enabled=True,
        channels=[
            {"channel_type": "email"},
            {"channel_type": "slack", "slack_channel_id": "C12345678"}
        ]
    )


class TestChannelConfig:
    """Test ChannelConfig model validation"""
    
    def test_email_channel_config_valid(self):
        """Email channel config should be valid without slack_channel_id"""
        config = ChannelConfig(channel_type=ChannelType.EMAIL)
        assert config.channel_type == ChannelType.EMAIL
        assert config.slack_channel_id is None
    
    def test_slack_channel_config_valid(self):
        """Slack channel config should be valid with slack_channel_id"""
        config = ChannelConfig(
            channel_type=ChannelType.SLACK,
            slack_channel_id="C12345678"
        )
        assert config.channel_type == ChannelType.SLACK
        assert config.slack_channel_id == "C12345678"
    
    def test_slack_channel_config_missing_id_invalid(self):
        """Slack channel config should fail validation without slack_channel_id"""
        with pytest.raises(ValueError, match="slack_channel_id is required"):
            ChannelConfig(channel_type=ChannelType.SLACK)
    
    def test_email_channel_config_with_slack_id_invalid(self):
        """Email channel config should fail validation with slack_channel_id"""
        with pytest.raises(ValueError, match="should not be provided"):
            ChannelConfig(
                channel_type=ChannelType.EMAIL,
                slack_channel_id="C12345678"
            )


class TestAlertHelperProperties:
    """Test Alert model helper properties for channel configs"""
    
    def test_has_email_channel(self, alert_with_email_only):
        """Alert should correctly identify email channel"""
        assert alert_with_email_only.has_email_channel is True
        assert alert_with_email_only.has_slack_channel is False
    
    def test_has_slack_channel(self, alert_with_slack_only):
        """Alert should correctly identify Slack channel"""
        assert alert_with_slack_only.has_email_channel is False
        assert alert_with_slack_only.has_slack_channel is True
    
    def test_has_both_channels(self, alert_with_both_channels):
        """Alert should correctly identify both channels"""
        assert alert_with_both_channels.has_email_channel is True
        assert alert_with_both_channels.has_slack_channel is True
    
    def test_slack_channel_ids_extraction(self, alert_with_slack_only):
        """Alert should extract slack_channel_ids correctly"""
        channel_ids = alert_with_slack_only.slack_channel_ids
        assert channel_ids == ["C12345678"]
    
    def test_slack_channel_ids_empty_for_email(self, alert_with_email_only):
        """Alert without Slack should return empty channel_ids list"""
        channel_ids = alert_with_email_only.slack_channel_ids
        assert channel_ids == []
    
    def test_channel_configs_default_to_email(self):
        """Alert without channels metadata should default to email"""
        alert = Alert(
            id=uuid4(),
            user_id=uuid4(),
            organisation_id=uuid4(),
            name="Test",
            alert_type=AlertType.NARRATIVE_VIEWS,
            scope=AlertScope.GENERAL,
            threshold=1000,
            enabled=True,
            metadata={}
        )
        assert alert.channel_configs == [{"channel_type": "email"}]
        assert alert.has_email_channel is True


class TestAlertRepositoryChannelDelivery:
    """Test AlertRepository.record_channel_delivery method"""
    
    @pytest.mark.asyncio
    async def test_record_email_delivery_success(self):
        """Should record email delivery status in JSONB field"""
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1
        
        repo = AlertRepository(mock_cursor)
        triggered_id = uuid4()
        
        result = await repo.record_channel_delivery(triggered_id, "email", "sent")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        
        # Verify the SQL includes JSONB update
        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0]
        assert "notification_status || " in sql.lower()
    
    @pytest.mark.asyncio
    async def test_record_slack_delivery_failed(self):
        """Should record Slack delivery failure in JSONB field"""
        mock_cursor = AsyncMock()
        mock_cursor.rowcount = 1
        
        repo = AlertRepository(mock_cursor)
        triggered_id = uuid4()
        
        result = await repo.record_channel_delivery(triggered_id, "slack", "failed")
        
        assert result is True
        mock_cursor.execute.assert_called_once()


class TestMultiChannelNotifications:
    """Test multi-channel notification flow in AlertService"""
    
    @pytest.mark.asyncio
    async def test_sends_to_both_email_and_slack(self, alert_with_both_channels):
        """Should send notifications to both email and Slack channels"""
        # This is a placeholder for integration test
        # Full implementation would require mocking email and Slack services
        assert alert_with_both_channels.has_email_channel
        assert alert_with_both_channels.has_slack_channel
        
    @pytest.mark.asyncio 
    async def test_slack_failure_does_not_block_email(self):
        """Slack delivery failure should not prevent email delivery"""
        # This is a placeholder for integration test
        # Would test that email is sent even if Slack fails
        pass
