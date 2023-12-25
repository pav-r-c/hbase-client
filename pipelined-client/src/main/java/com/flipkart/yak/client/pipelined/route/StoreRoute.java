package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.models.ReadConsistency;
import com.flipkart.yak.client.pipelined.models.Region;
import com.flipkart.yak.client.pipelined.models.WriteConsistency;

@SuppressWarnings("java:S3740")
public class StoreRoute extends Route {

  private final ReadConsistency readConsistency;

  private final WriteConsistency writeConsistency;

  public StoreRoute(Region region, ReadConsistency readConsistency, WriteConsistency writeConsistency,
                    HotRouter hotRouter) {
    super(region, hotRouter);
    this.readConsistency = readConsistency;
    this.writeConsistency = writeConsistency;
  }

  public ReadConsistency getReadConsistency() {
    return readConsistency;
  }

  public WriteConsistency getWriteConsistency() {
    return writeConsistency;
  }
}