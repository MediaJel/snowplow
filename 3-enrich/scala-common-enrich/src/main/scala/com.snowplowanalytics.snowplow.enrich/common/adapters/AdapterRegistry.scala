/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common
package adapters

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.Client
import io.circe.Json

import loaders.CollectorPayload
import registry._
import registry.snowplow.{
  Tp1Adapter => SpTp1Adapter,
  Tp2Adapter => SpTp2Adapter,
  RedirectAdapter => SpRedirectAdapter
}

/**
 * The AdapterRegistry lets us convert a CollectorPayload into one or more RawEvents, using a given
 * adapter.
 */
object AdapterRegistry {

  private object Vendor {
    val Snowplow = "com.snowplowanalytics.snowplow"
    val Redirect = "r"
    val Iglu = "com.snowplowanalytics.iglu"
    val Callrail = "com.callrail"
    val Cloudfront = "com.amazon.aws.cloudfront"
    val GoogleAnalytics = "com.google.analytics"
    val Mailchimp = "com.mailchimp"
    val Mailgun = "com.mailgun"
    val Mandrill = "com.mandrill"
    val Olark = "com.olark"
    val Pagerduty = "com.pagerduty"
    val Pingdom = "com.pingdom"
    val Sendgrid = "com.sendgrid"
    val StatusGator = "com.statusgator"
    val Unbounce = "com.unbounce"
    val UrbanAirship = "com.urbanairship.connect"
    val Marketo = "com.marketo"
    val Vero = "com.getvero"
    val HubSpot = "com.hubspot"
  }

  /**
   * Router to determine which adapter we use to convert the CollectorPayload into one or more
   * RawEvents.
   * @param payload The CollectorPayload we are transforming
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Strings on
   * Failure
   */
  def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] =
    (payload.api.vendor, payload.api.version) match {
      case (Vendor.Snowplow, "tp1") => SpTp1Adapter.toRawEvents(payload, client)
      case (Vendor.Snowplow, "tp2") => SpTp2Adapter.toRawEvents(payload, client)
      case (Vendor.Redirect, "tp2") => SpRedirectAdapter.toRawEvents(payload, client)
      case (Vendor.Iglu, "v1") => IgluAdapter.toRawEvents(payload, client)
      case (Vendor.Callrail, "v1") => CallrailAdapter.toRawEvents(payload, client)
      case (Vendor.Cloudfront, "wd_access_log") =>
        CloudfrontAccessLogAdapter.WebDistribution.toRawEvents(payload, client)
      case (Vendor.Mailchimp, "v1") => MailchimpAdapter.toRawEvents(payload, client)
      case (Vendor.Mailgun, "v1") => MailgunAdapter.toRawEvents(payload, client)
      case (Vendor.GoogleAnalytics, "v1") => GoogleAnalyticsAdapter.toRawEvents(payload, client)
      case (Vendor.Mandrill, "v1") => MandrillAdapter.toRawEvents(payload, client)
      case (Vendor.Olark, "v1") => OlarkAdapter.toRawEvents(payload, client)
      case (Vendor.Pagerduty, "v1") => PagerdutyAdapter.toRawEvents(payload, client)
      case (Vendor.Pingdom, "v1") => PingdomAdapter.toRawEvents(payload, client)
      case (Vendor.Sendgrid, "v3") => SendgridAdapter.toRawEvents(payload, client)
      case (Vendor.StatusGator, "v1") => StatusGatorAdapter.toRawEvents(payload, client)
      case (Vendor.Unbounce, "v1") => UnbounceAdapter.toRawEvents(payload, client)
      case (Vendor.UrbanAirship, "v1") => UrbanAirshipAdapter.toRawEvents(payload, client)
      case (Vendor.Marketo, "v1") => MarketoAdapter.toRawEvents(payload, client)
      case (Vendor.Vero, "v1") => VeroAdapter.toRawEvents(payload, client)
      case (Vendor.HubSpot, "v1") => HubSpotAdapter.toRawEvents(payload, client)
      case _ =>
        Monad[F].pure(
          (s"Payload with vendor ${payload.api.vendor} and version ${payload.api.version} not " +
            "supported by this version of Scala Common Enrich").invalidNel)
    }

}
