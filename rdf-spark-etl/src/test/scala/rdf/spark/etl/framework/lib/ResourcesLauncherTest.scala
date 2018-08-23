package rdf.spark.etl.framework.lib

import org.scalatest.{FlatSpec, Matchers}

class ResourcesLauncherTest extends FlatSpec with Matchers {
	
	"Get resource" should "send value if it exists" in {
		val r = new ResourcesLauncher("dev")
		r.get("job.name") shouldBe a [Some[_]]
	}
	
	it should "send None value if it does not exist" in {
		val r = new ResourcesLauncher("dev")
		r.get("job.nameeee") should be (None)
	}
	
}
