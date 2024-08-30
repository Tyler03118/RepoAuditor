# -------------------------------------------------------------------------------
# |
# |  Copyright (c) 2024 Scientific Software Engineering Center at Georgia Tech
# |  Distributed under the MIT License.
# |
# -------------------------------------------------------------------------------
"""Contains the ClassicBranchProtection object"""

from typing import Any, Optional

from dbrownell_Common.Types import override  # type: ignore[import-untyped]

from RepoAuditor.Query import ExecutionStyle, Query

from .ClassicBranchProtectionRequirements.AllowDeletions import AllowDeletions
from .ClassicBranchProtectionRequirements.AllowForcePushes import AllowForcePushes
from .ClassicBranchProtectionRequirements.DismissStalePullRequestApprovals import (
    DismissStalePullRequestApprovals,
)
from .ClassicBranchProtectionRequirements.DoNotAllowBypassSettings import DoNotAllowBypassSettings
from .ClassicBranchProtectionRequirements.EnsureStatusChecks import EnsureStatusChecks
from .ClassicBranchProtectionRequirements.RequireApprovalMostRecentPush import (
    RequireApprovalMostRecentPush,
)
from .ClassicBranchProtectionRequirements.RequireApprovals import RequireApprovals
from .ClassicBranchProtectionRequirements.RequireCodeOwnerReview import RequireCodeOwnerReview
from .ClassicBranchProtectionRequirements.RequireConversationResolution import (
    RequireConversationResolution,
)
from .ClassicBranchProtectionRequirements.RequireLinearHistory import RequireLinearHistory
from .ClassicBranchProtectionRequirements.RequirePullRequests import RequirePullRequests
from .ClassicBranchProtectionRequirements.RequireSignedCommits import RequireSignedCommits
from .ClassicBranchProtectionRequirements.RequireStatusChecksToPass import RequireStatusChecksToPass
from .ClassicBranchProtectionRequirements.RequireUpToDateBranches import RequireUpToDateBranches

# REQUEST : https://api.github.com/repos/<username>/<repo>/branches/<branch>/protection
# RESPONSE:
# {
#     "url": <not used>,
#     "required_status_checks": { /* RequireStatusChecksToPass */
#         "url": <not used>,
#         "strict": RequireUpToDateBranches,
#         "contexts": <not used>,
#         "contexts_url": <not used>,
#         "checks": EnsureStatusChecks
#     },
#     "required_pull_request_reviews": {  /* RequirePullRequests */
#         "url": <not used>,
#         "dismiss_stale_reviews": DismissStalePullRequestApprovals,
#         "require_code_owner_reviews": RequireCodeOwnerReview,
#         "require_last_push_approval": RequireApprovalMostRecentPush,
#         "required_approving_review_count": RequireApprovals,
#         "bypass_pull_request_allowances": <not used>,
#     },
#     "required_signatures": {
#         "url": <not used>,
#         "enabled": RequireSignedCommits
#     },
#     "enforce_admins": {
#         "url": <not used>,
#         "enabled": DoNotAllowBypassSettings
#     },
#     "required_linear_history": {
#         "enabled": RequireLinearHistory
#     },
#     "allow_force_pushes": {
#         "enabled": AllowForcePushes
#     },
#     "allow_deletions": {
#         "enabled": AllowDeletions
#     },
#     "block_creations": {
#         "enabled": <not used>
#     },
#     "required_conversation_resolution": {
#         "enabled": RequireConversationResolution
#     },
#     "lock_branch": {
#         "enabled": <not used>
#     },
#     "allow_fork_syncing": {
#         "enabled": <not used>
#     }
# }


# ----------------------------------------------------------------------
class ClassicBranchProtectionQuery(Query):
    """Query with requirements that operate on class branch protection rules."""

    # ----------------------------------------------------------------------
    def __init__(self) -> None:
        super(ClassicBranchProtectionQuery, self).__init__(
            "ClassicBranchProtectionQuery",
            ExecutionStyle.Parallel,
            [
                AllowDeletions(),
                AllowForcePushes(),
                DismissStalePullRequestApprovals(),
                DoNotAllowBypassSettings(),
                EnsureStatusChecks(),
                RequireApprovalMostRecentPush(),
                RequireApprovals(),
                RequireCodeOwnerReview(),
                RequireConversationResolution(),
                RequireLinearHistory(),
                RequirePullRequests(),
                RequireSignedCommits(),
                RequireStatusChecksToPass(),
                RequireUpToDateBranches(),
            ],
        )

    # ----------------------------------------------------------------------
    @override
    def GetData(
        self,
        module_data: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        branch = module_data.get("branch", None)
        if branch is None:
            # Get the default branch name
            response = module_data["session"].get("")

            response.raise_for_status()
            response = response.json()

            branch = response["default_branch"]

        module_data["branch"] = branch

        # Get the information for the default branch
        response = module_data["session"].get(f"branches/{module_data['branch']}")

        response.raise_for_status()
        response = response.json()

        module_data["branch_data"] = response

        if not module_data["branch_data"].get("protected", False):
            return None

        # Note that once here, we know that the branch is protected, but we don't know the
        # protection scheme used (rule sets or classic). Attempt to get the classic information
        # and then see if rule sets are in use if the classic information is not found.
        response = module_data["session"].get(f"/branches/{module_data['branch']}/protection")

        if response.status_code == 404:
            # Does this branch use rule sets?
            ruleset_response = module_data["session"].get(f"rules/branches/{module_data['branch']}")

            ruleset_response.raise_for_status()
            ruleset_response = ruleset_response.json()

            # If there is data, assume that the branch is protected by rule sets
            if ruleset_response:
                return None

            # Classic branch protection information is only accessible when a pat is provided. Exit
            # gracefully if there isn't a PAT, as the DefaultBranchQuery will print a warning if
            # the PAT wasn't provided.
            if module_data["session"].github_pat is None:
                return None

            # If here, let the error result in an exception in the code that follows.

        response.raise_for_status()
        response = response.json()

        module_data["branch_protection_data"] = response

        return module_data
