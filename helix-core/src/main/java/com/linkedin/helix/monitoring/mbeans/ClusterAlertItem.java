package com.linkedin.helix.monitoring.mbeans;


import java.util.Date;

import com.linkedin.helix.alerts.AlertValueAndStatus;

public class ClusterAlertItem implements ClusterAlertItemMBean
{
  String _alertItemName;
  double  _alertValue;
  int _alertFired;
  String _additionalInfo = "";
  AlertValueAndStatus _valueAndStatus;
  long _lastUpdateTime = 0;
  
  public ClusterAlertItem(String name, AlertValueAndStatus valueAndStatus)
  {
    _valueAndStatus = valueAndStatus;
    _alertItemName = name;
    refreshValues();
  }
  @Override
  public String getSensorName()
  {
    return _alertItemName;
  }

  @Override
  public double getAlertValue()
  {
    return _alertValue;
  }
  
  public void setValueMap(AlertValueAndStatus valueAndStatus)
  {
    _valueAndStatus = valueAndStatus;
    refreshValues();
  }
  
  void refreshValues()
  {
    _lastUpdateTime = new Date().getTime();
    if(_valueAndStatus.getValue().getElements().size() > 0)
    {
      _alertValue = Double.parseDouble(_valueAndStatus.getValue().getElements().get(0));
    }
    else
    {
      _alertValue = 0;
    }
    _alertFired = _valueAndStatus.isFired() ?  1 : 0;
  }
  @Override
  public int getAlertFired()
  {
    return _alertFired;
  }
  
  public void setAdditionalInfo(String additionalInfo)
  {
    _additionalInfo = additionalInfo;
  }
  
  @Override
  public String getAdditionalInfo()
  {
    return _additionalInfo;
  }
  
  public void reset()
  {
    _alertFired = 0;
    _additionalInfo = "";
    _alertValue = 0;
  }
  
  public long getLastUpdateTime()
  {
    return _lastUpdateTime;
  }
}
