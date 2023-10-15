import fs from "fs";
import csv from "csv-parser";
import { CheerioCrawler } from "crawlee";

//Required Interfaces
interface Profile {
  name: String;
  description:String;
  founded: number;
  teamSize: number;
  jobs: jobs[];
  founders: founders[];
  launchPosts: launchPosts[];
  LinkedIn: String;
  Twitter: String;
  Facebook: String;
  GitHub: String;
  location:String;
  country:String;
}

interface jobs {
  role: String;
  location: String;
  type: String;
  experience: String;
}

interface Company {
  name: string;
  url: string;
}
interface founders {
  name: string;
  linkedIn: string;
}
interface launchPosts {
  title: String;
  body: String;
}

// Reading companies and its url using given csv file
async function readingCompanies(): Promise<Company[]> {
  const companies: Company[] = [];

  return new Promise((resolve) => {
    fs.createReadStream("inputs/companies.csv")
      .pipe(csv())
      .on("data", (row: any) => {
        const company: Company = {
          name: row["Company Name"],
          url: row["YC URL"],
        };
        //console.log(company)
        companies.push(company);
      })
      .on("end", () => {
        resolve(companies);
      });
  });
}

async function scrapeProfile(company: Company): Promise<Profile> {
  // const project = new Crawlee();
  // Extracting and structuring the data into a Profile interface
  const Profile: Profile = {
    name: company.name,
    description:"",
    founded: 0,
    teamSize: 0,
    jobs: [],
    founders: [],
    launchPosts: [],
    LinkedIn: "",
    Twitter: "",
    Facebook: "",
    GitHub: "",
    location:"",
    country:"",
    // We can add more fields as needed here
  };
  const crawler = new CheerioCrawler({
    // Using the requestHandler to process each of the crawled pages.
    async requestHandler({ request, $, enqueueLinks, log }) {
      const title = $("title").text();
      const h = $("script.js-react-on-rails-component").text();
      const jsonData = JSON.parse(h);
      // console.log(jsonData);
      const longDescription = jsonData.company.long_description;
      const founded = jsonData.company.year_founded;
      const teamSize = jsonData.company.team_size;

      const location = jsonData.company.location;
      const country = jsonData.company.country;
      const LinkedIn = jsonData.company.linkedin_url;
      const Twitter = jsonData.company.twitter_url;
      const Facebook = jsonData.company.fb_url;
      const GitHub = jsonData.company.github_url;

      Profile.name = title.split(":")[0];
      Profile.description = longDescription;
      Profile.founded = founded;
      Profile.teamSize = teamSize;

      for (const x of jsonData.jobPostings) {
        const role = x.title;
        const location = x.location;
        const type = x.type;
        const experience = x.minExperience;
        Profile.jobs.push({ role, location, type, experience });
      }
      for (const x of jsonData.company.founders) {
        const name = x.full_name;
        const linkedIn = x.linkedin_url;
        Profile.founders.push({ name, linkedIn });
      }
      for (const x of jsonData.launches) {
        const title = x.title;
        const body = x.body;
        Profile.launchPosts.push({ title, body });
      }
      Profile.LinkedIn = LinkedIn;
      Profile.Twitter = Twitter;
      Profile.Facebook = Facebook;
      Profile.GitHub = GitHub;
      Profile.location = location;
      Profile.country = country;

      // Extracting links from the current page
      // and adding them to the crawling queue.
      await enqueueLinks();
    },

    // limiting our crawls
    maxRequestsPerCrawl: 100,
  });

  const arr: any = [];
  arr.push(company.url);
  await crawler.run([company.url]);
  return Profile;
}

//writing to our required output file i.e scraped.json
async function writeScrapedDataToJSON(data: any[]) {
  const outputFileName = "out/scraped.json";
  const dirname = "out";
  if (!fs.existsSync(dirname)) {
    fs.mkdirSync(dirname, { recursive: true });
  }
  fs.writeFileSync(outputFileName, JSON.stringify(data, null, 2));
}

// Main function
export async function processCompanyList() {
  const companies = await readingCompanies();
  //console.log(companies)
  const scrapedData: any[] = [];

  for (const company of companies) {
    const profileData = await scrapeProfile(company);
    scrapedData.push(profileData);
  }
  await writeScrapedDataToJSON(scrapedData);
}

// Call the main function to start the process
// processCompanyList()
