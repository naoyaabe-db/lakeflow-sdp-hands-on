def get_raw_traffic_log_rules():
  return {
      # セッション時間の最大値は3600秒未満とし、3600秒超は不正データとみなす
      "session_less_than_3000": "session_duration <= 3600"
  }

def get_joined_traffic_log_rules():
  return {
      # 通信ログに基地局マスタを左外部結合した時、cell_master側のPrimary Keyである
      # cell_idがNULLである場合(つまり紐付けられなかった場合) は、
      # 基地局マスタに存在しない基地局の通信ログとなるため、不正データとみなす
      "cell_id_is_not_null": "cell_master.cell_id IS NOT NULL",
      # 通信ログに加入者マスタを左外部結合した時、cell_master側のPrimary Keyである
      # imsiがNULLである場合(つまり紐付けられなかった場合) は、
      # 加入者マスタに存在しない加入者の通信ログとなるため、不正データとみなす
      "imsi_is_not_null": "subscriber_master.imsi IS NOT NULL",
  }