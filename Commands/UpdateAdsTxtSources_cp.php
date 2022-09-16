<?php

namespace App\Console\Commands\SourceCrawler;

use App\Jobs\AdsTxtSourceJob;
use App\Models\AdsTxtSources;
use Carbon\Carbon;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Queue;
use Illuminate\Database\Eloquent\Collection;

class UpdateAdsTxtSources extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */

    protected $signature = 'ads-txt-sources-update:start';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Get domains from `ads_txt_sources`, parse new data and update `ads_txt_sources`, `ads_txt_contents`';


    private int $countGetRows = 1000;
    private int $limitJobs = 300000;
    private string $weekAgo;
    private Carbon $carbon;
    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct(Carbon $carbon)
    {
        parent::__construct();
        $this->carbon = $carbon;
    }


    /**
     * @throws Exception
     */
    private function init()
    {
        $queues = [
            'adsTxtSourcesUpdate'
        ];
        $counters = [];
        foreach ($queues as $queue) {
            $counters[$queue] = Queue::getRedis()->command('LLEN', ["queues:{$queue}"]);
            $counters[$queue . ':reserved'] = Queue::getRedis()->command('ZCARD', ["queues:{$queue}:reserved"]);
        }

        if ($this->limitJobs < $counters['adsTxtSourcesUpdate']) {
            throw new Exception("Too many jobs");
        }

        $this->weekAgo = $this->carbon::now()->subDays(7)->toDateString();
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $this->init();
        $this->prepareListNotScannedAndError();
        $this->prepareListScanned();
        return 0;
    }


    private function prepareListNotScannedAndError() {

        (new AdsTxtSources)
            ->select('id', 'domain', 'path_to_file')
            ->whereJsonLength('crawling_status', 0)
            ->orWhere('crawling_status->status', 'fail')
            ->orderBy('id', "ASC")
            ->chunk($this->countGetRows, function ($sources) {
                $this->sendJobs($sources);
            });
    }

    private function prepareListScanned()
    {
        (new AdsTxtSources)
            ->select('id', 'domain', 'path_to_file')
            ->where('crawling_status->status', 'success')
            ->orderBy('last_visited', 'ASC')
            ->chunk($this->countGetRows, function ($sources) {
                $this->sendJobs($sources);
            });
    }

    private function sendJobs(Collection $sources) {
        $sourcesArray = $sources->toArray();
        foreach ($sourcesArray as $source) {
            AdsTxtSourceJob::dispatch($source)->onQueue('adsTxtSourcesUpdate');
        }
    }
}
