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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.URI

import cats.Eval
import cats.data.EitherT
import com.snowplowanalytics.refererparser._
import org.specs2.Specification
import org.specs2.matcher.DataTables

/**
 * A small selection of tests partially borrowed from referer-parser.
 * This is a very imcomplete set - more a tripwire than an exhaustive test.
 */
class ExtractRefererDetailsSpec extends Specification with DataTables {
  def is = s2"""
  This is a specification to test extractRefererDetails
  Parsing referer URIs should work                     $e1
  Tabs and newlines in search terms should be replaced $e2
  """

  val PageHost = "www.snowplowanalytics.com"

  def e1 =
    "SPEC NAME" || "REFERER URI" | "REFERER" |
      "Google search" !! "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari" ! SearchReferer(Medium.Search, "Google", Some("gateway oracle cards denise linn")) |
      "Facebook social" !! "http://www.facebook.com/l.php?u=http%3A%2F%2Fwww.psychicbazaar.com&h=yAQHZtXxS&s=1" ! SocialReferer(Medium.Social, "Facebook") |
      "Yahoo! Mail" !! "http://36ohk6dgmcd1n-c.c.yom.mail.yahoo.net/om/api/1.0/openmail.app.invoke/36ohk6dgmcd1n/11/1.0.35/us/en-US/view.html/0" ! EmailReferer(Medium.Email, "Yahoo! Mail") |
      "Internal referer" !! "https://www.snowplowanalytics.com/account/profile" ! InternalReferer(Medium.Internal) |
      "Custom referer" !! "https://www.internaldomain.com/path" ! InternalReferer(Medium.Internal) |
      "Unknown referer" !! "http://www.spyfu.com/domain.aspx?d=3897225171967988459" ! UnknownReferer(Medium.Unknown) |> {
      (_, refererUri, referer) => (for {
        c <- EitherT.rightT[Eval, String](RefererParserConf(
          (new URI("http://snowplow.com"), "referer.json"),
          List("www.internaldomain.com")
        ))
        e <- c.enrichment[Eval]
        res = e.extractRefererDetails(new URI(refererUri), PageHost)
      } yield res).value.value must beRight.like {
        case o => o must_== Some(referer)
      }
    }

  def e2 = (for {
    c <- EitherT.rightT[Eval, String](RefererParserConf(
      (new URI("http://snowplow.com"), "referer.json"),
      List()
    ))
    e <- c.enrichment[Eval]
    res = e.extractRefererDetails(new URI(
      "http://www.google.com/search?q=%0Agateway%09oracle%09cards%09denise%09linn&hl=en&client=safari"), PageHost)
  } yield res).value.value must beRight.like {
    case o => o must_== Some(
      SearchReferer(Medium.Search, "Google", Some("gateway    oracle    cards    denise    linn")))
  }
}
