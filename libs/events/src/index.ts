export type LoanRequested = {
  event: "loan.requested";
  data: {
    applicationId: string;
    userId: string;
    amount: number;
    product: string;
    type: "UNSECURED" | "SECURED";
    requestedAt: string;
    metadata?: Record<string, any>;
  };
};

export type KycCompleted = {
  event: "kyc.completed";
  data: {
    applicationId: string;
    userId: string;
    kycStatus: "PASSED" | "FAILED";
    details?: any;
    checkedAt: string;
  };
};

export type CreditChecked = {
  event: "credit.checked";
  data: {
    applicationId: string;
    userId: string;
    score: number;
    decision: "PASS" | "REVIEW" | "FAIL";
    checkedAt: string;
    raw?: any;
  };
};

export type RiskChecked = {
  event: "risk.checked";
  data: {
    applicationId: string;
    userId: string;
    risk: "LOW" | "MEDIUM" | "HIGH";
    details?: any;
    checkedAt: string;
  };
};

export type BlacklistChecked = {
  event: "blacklist.checked";
  data: {
    applicationId: string;
    userId: string;
    blacklisted: boolean;
    reason?: string;
    checkedAt: string;
  };
};

export type LoanCancelled = {
  event: "loan.cancelled";
  data: {
    applicationId: string;
    reason: string;
    cancelledAt: string;
  };
};

export type LoanApproved = {
  event: "loan.approved";
  data: {
    applicationId: string;
    approvedAt: string;
    note?: string;
  };
};

export type AuditLog = {
  event: "audit.logged";
  data: {
    applicationId?: string;
    eventName: string;
    payload: any;
    recordedAt: string;
  };
};
