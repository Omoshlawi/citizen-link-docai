from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models.pipeline import ConversationEntry


class AgentExhaustedError(RuntimeError):
    """
    Raised when an agent exhausts all correction attempts without producing
    a valid output. Carries the full conversation trail so the pipeline can
    persist every round even on total failure.
    """

    def __init__(self, message: str, conversation: list[ConversationEntry]) -> None:
        super().__init__(message)
        self.conversation = conversation
