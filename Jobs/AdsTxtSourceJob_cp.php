<?php

namespace App\Jobs;

use App\Services\Crawler\AdsTxtSourceService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class AdsTxtSourceJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected array $sourceData;
    public int $tries = 1;
    public int $maxExceptions = 3;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(array $sourceData)
    {
        $this->sourceData = $sourceData;
        $this->onQueue('adsTxtSourcesUpdate');
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle(AdsTxtSourceService $adsTxtSourceService)
    {
        $adsTxtSourceService->setSource($this->sourceData)->parseAndSaveSource();
    }
}
